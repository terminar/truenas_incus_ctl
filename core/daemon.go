package core

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const TNC_PREFIX_STRING = "tnc_daemon."
const JOB_WAIT_STRING = "core.job_wait"
const DEFAULT_CALL_TIMEOUT = "30s" // also see: cmd.defaultCallTimeout
const DEFAULT_SESSION_IDLE_TIMEOUT = 2 * time.Minute
const MAX_CALL_ATTEMPTS = 3

// WebSocket keepalive. The read deadline is only enforced once the server has answered a ping,
// so a server that doesn't reply to pings never gets its connections dropped.
var sessionPingInterval = 30 * time.Second
var sessionReadTimeout = 90 * time.Second
var sessionReaperInterval = 15 * time.Second

const sessionLogoutTimeout = "5s"
const sessionCloseGracePeriod = time.Second

type TruenasSession struct {
	url          string
	conn         *websocket.Conn
	ctx          *DaemonContext
	sessionKey   string
	channel      int
	future       *Future[*TruenasSession]
	connMtx      *sync.Mutex
	writeMtx     *sync.Mutex
	doneCh       chan struct{}
	pingInterval time.Duration
	readTimeout  time.Duration
	inFlight_    int
	lastUsed_    time.Time
	closing_     bool
	curCallId_   int64
	callMap_     map[int64]*Future[json.RawMessage]
	jobMap_      map[int64]*Future[json.RawMessage]
}

type DaemonContext struct {
	timeoutValue       time.Duration
	timeoutTimer       *time.Timer
	sessionIdleTimeout time.Duration
	timerMtx           *sync.Mutex
	activeRequests_    int
	mapMtx             *sync.Mutex
	sessionMap_        map[string][]*Future[*TruenasSession]
}

type CallInfo struct {
	method string
	params []interface{}
}

type LoginInfo struct {
	call          CallInfo
	serverUrl     string
	allowInsecure bool
}

func newDaemonContext(daemonTimeout time.Duration, sessionIdleTimeout time.Duration) *DaemonContext {
	var timer *time.Timer
	if daemonTimeout != 0 {
		timer = time.NewTimer(daemonTimeout)
	}
	return &DaemonContext{
		timeoutValue:       daemonTimeout,
		timeoutTimer:       timer,
		sessionIdleTimeout: sessionIdleTimeout,
		timerMtx:           &sync.Mutex{},
		mapMtx:             &sync.Mutex{},
		sessionMap_:        make(map[string][]*Future[*TruenasSession]),
	}
}

func RunDaemon(serverSockAddr string, globalTimeoutStr string, sessionIdleTimeoutStr string) {
	var err error
	var daemonTimeout time.Duration
	if globalTimeoutStr != "" {
		daemonTimeout, err = time.ParseDuration(globalTimeoutStr)
		if err != nil {
			log.Fatal("Error: could not parse duration \"" + globalTimeoutStr + "\":" + err.Error())
		}
	}
	sessionIdleTimeout := DEFAULT_SESSION_IDLE_TIMEOUT
	if sessionIdleTimeoutStr != "" {
		sessionIdleTimeout, err = time.ParseDuration(sessionIdleTimeoutStr)
		if err != nil {
			log.Fatal("Error: could not parse duration \"" + sessionIdleTimeoutStr + "\":" + err.Error())
		}
	}

	fmt.Println("Serving on", serverSockAddr)
	if daemonTimeout != 0 {
		fmt.Println("With a daemon timeout of", daemonTimeout.String())
	}
	if sessionIdleTimeout > 0 {
		fmt.Println("Closing TrueNAS sessions after being idle for", sessionIdleTimeout.String())
	}

	ls, err := net.Listen("unix", serverSockAddr)
	if err != nil {
		fmt.Println("Listen error:", err)
		return
	}

	daemon := newDaemonContext(daemonTimeout, sessionIdleTimeout)

	if sessionIdleTimeout > 0 {
		go func() {
			ticker := time.NewTicker(sessionReaperInterval)
			for range ticker.C {
				daemon.reapIdleSessions()
			}
		}()
	}

	doneCh := make(chan os.Signal, 1)
	signal.Notify(doneCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if daemon.timeoutTimer != nil {
			select {
			case <-daemon.timeoutTimer.C:
				log.Println("tncdaemon timed out (" + daemonTimeout.String() + " elapsed)")
				break
			case <-doneCh:
				log.Println("tncdaemon exiting")
				break
			}
		} else {
			<-doneCh
		}
		_ = ls.Close()
		daemon.closeAllSessions()
		os.Remove(serverSockAddr)
		os.Exit(0)
	}()

	http.Serve(ls, daemon)

	doneCh <- syscall.SIGTERM
}

// The daemon timeout only counts time without any client requests in flight.
// Messages from TrueNAS (e.g. job events) do not keep the daemon alive.
func (d *DaemonContext) requestStarted() {
	d.timerMtx.Lock()
	defer d.timerMtx.Unlock()
	d.activeRequests_++
	if d.timeoutTimer != nil {
		d.timeoutTimer.Stop()
	}
}

func (d *DaemonContext) requestFinished() {
	d.timerMtx.Lock()
	defer d.timerMtx.Unlock()
	d.activeRequests_--
	if d.activeRequests_ <= 0 && d.timeoutTimer != nil {
		d.activeRequests_ = 0
		d.timeoutTimer.Reset(d.timeoutValue)
	}
}

func (d *DaemonContext) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.requestStarted()
	defer d.requestFinished()

	out, err := d.serveImpl(r)
	if err != nil {
		//log.Println(err)
		w.WriteHeader(500)
		io.WriteString(w, err.Error())
	} else {
		//log.Println(string(out))
		w.WriteHeader(200)
		w.Write(out)
	}
}

func (d *DaemonContext) serveImpl(r *http.Request) (json.RawMessage, error) {
	host := r.Header.Get("TNC-Host-Url")
	key := r.Header.Get("TNC-Api-Key")
	user := r.Header.Get("TNC-Username")
	pass := r.Header.Get("TNC-Password")
	method := r.Header.Get("TNC-Call-Method")
	timeoutStr := r.Header.Get("TNC-Timeout")
	allowInsecure := false
	if str := r.Header.Get("TNC-Allow-Insecure"); str != "" {
		allowInsecure = strings.ToLower(str) == "true"
	}

	log.Println("Received request at", r.URL.String(), "for method", method)

	if method == "" {
		return nil, fmt.Errorf("TNC-Call-Method was not provided")
	}

	if method == TNC_PREFIX_STRING+"ping" {
		return []byte("\"pong\""), nil
	}

	if host == "" {
		return nil, fmt.Errorf("TNC-Host-Url was not provided")
	}

	var sessionKey string
	var methodLogin string
	var paramsLogin []interface{}

	if key == "" {
		if user == "" || pass == "" {
			return nil, fmt.Errorf("TNC-Api-Key was not provided, nor TNC-Username nor TNC-Password")
		}
		sessionKey = host + user + pass
		methodLogin = "auth.login"
		paramsLogin = []interface{}{user, pass}
	} else {
		sessionKey = host + key
		methodLogin = "auth.login_with_api_key"
		paramsLogin = []interface{}{key}
	}
	sessionKey += fmt.Sprint(allowInsecure)

	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	var params []interface{}
	if err = json.Unmarshal(data, &params); err != nil {
		return nil, err
	}

	call := CallInfo{
		method: method,
		params: params,
	}
	login := LoginInfo{
		call: CallInfo{
			method: methodLogin,
			params: paramsLogin,
		},
		serverUrl:     host,
		allowInsecure: allowInsecure,
	}

	var out json.RawMessage
	for attempt := 1; attempt <= MAX_CALL_ATTEMPTS; attempt++ {
		var shouldRetry bool
		out, err, shouldRetry = d.maybeCreateSessionAndCall(sessionKey, timeoutStr, call, login)
		if !shouldRetry {
			break
		}
		log.Println("Daemon: retrying", method, "on a fresh connection (attempt", attempt+1, "of", MAX_CALL_ATTEMPTS, ")")
	}
	return out, err
}

func (d *DaemonContext) maybeCreateSessionAndCall(sessionKey string, timeoutStr string, call CallInfo, login LoginInfo) (json.RawMessage, error, bool) {
	s, err := d.acquireSession(sessionKey, login)
	if err != nil {
		return nil, err, false
	}

	//log.Println("Calling method", method)

	out, err, shouldRetry := s.callJson(call.method, timeoutStr, call.params)
	s.release()

	if !shouldRetry && err == nil && IsNotAuthenticatedResponse(out) {
		log.Println("Daemon: TrueNAS session is no longer authenticated, reconnecting")
		shouldRetry = true
	}
	if shouldRetry {
		d.discardSession(s, false)
	}

	return out, err, shouldRetry
}

// acquireSession reserves an idle connection for sessionKey, or creates a new one.
// The returned session must be handed back with release().
func (d *DaemonContext) acquireSession(sessionKey string, login LoginInfo) (*TruenasSession, error) {
	d.mapMtx.Lock()
	futureSessions := d.sessionMap_[sessionKey]
	channel := -1
	for i := 0; i < len(futureSessions); i++ {
		if futureSessions[i] == nil {
			if channel < 0 {
				channel = i
			}
			continue
		}
		done, session, err := futureSessions[i].Peek()
		if !done || err != nil || session == nil {
			continue
		}
		if session.tryReserve() {
			d.mapMtx.Unlock()
			return session, nil
		}
	}
	if channel < 0 {
		channel = len(futureSessions)
		futureSessions = append(futureSessions, nil)
	}
	future := MakeFuture[*TruenasSession]()
	futureSessions[channel] = future
	d.sessionMap_[sessionKey] = futureSessions
	d.mapMtx.Unlock()

	s, err := d.createSession(sessionKey, login, channel, future)
	if err == nil && s == nil {
		err = fmt.Errorf("Failed to launch TrueNAS client session (unknown error)")
	}
	if err != nil {
		future.Fail(err)
		d.removeSession(sessionKey, channel, future)
		return nil, err
	}
	future.Complete(s)
	return s, nil
}

func (d *DaemonContext) createSession(sessionKey string, login LoginInfo, channel int, future *Future[*TruenasSession]) (*TruenasSession, error) {
	u, err := url.Parse(login.serverUrl)
	if err != nil {
		return nil, fmt.Errorf("Invalid URL: %w", err)
	}

	log.Println("Daemon: creating connection with allowInsecure=" + fmt.Sprint(login.allowInsecure))

	// Configure WebSocket connection with insecure TLS to accept self-signed certs
	dialer := &websocket.Dialer{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: login.allowInsecure,
		},
		Proxy: http.ProxyFromEnvironment,
	}

	// Establish the WebSocket connection
	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect: %w", err)
	}

	session := &TruenasSession{
		url:          login.serverUrl,
		conn:         conn,
		ctx:          d,
		sessionKey:   sessionKey,
		channel:      channel,
		future:       future,
		connMtx:      &sync.Mutex{},
		writeMtx:     &sync.Mutex{},
		doneCh:       make(chan struct{}),
		pingInterval: sessionPingInterval,
		readTimeout:  sessionReadTimeout,
		inFlight_:    1, // reserved for the request that created it
		lastUsed_:    time.Now(),
		curCallId_:   0,
		callMap_:     make(map[int64]*Future[json.RawMessage]),
		jobMap_:      make(map[int64]*Future[json.RawMessage]),
	}

	go session.listen()
	go session.keepAlive()

	out, err, _ := session.callJson(login.call.method, DEFAULT_CALL_TIMEOUT, login.call.params)
	if err == nil {
		err = checkLoginResponse(out)
	}
	if err != nil {
		session.shutdown(false)
		return nil, err
	}

	_, err, _ = session.callJson("core.subscribe", DEFAULT_CALL_TIMEOUT, []interface{}{"core.get_jobs"})
	if err != nil {
		session.shutdown(true)
		return nil, err
	}

	return session, nil
}

func checkLoginResponse(out json.RawMessage) error {
	var response map[string]interface{}
	if err := json.Unmarshal(out, &response); err != nil {
		return fmt.Errorf("Login failed: could not parse response: %w", err)
	}
	if errValue, exists := response["error"]; exists && errValue != nil {
		return fmt.Errorf("Login failed: %s", ExtractApiErrorJsonGivenError(errValue))
	}
	if ok, _ := response["result"].(bool); !ok {
		return fmt.Errorf("Login failed: TrueNAS rejected the credentials")
	}
	return nil
}

// IsNotAuthenticatedResponse reports whether TrueNAS rejected a call because the
// connection is no longer logged in (e.g. after the middleware expired the session).
func IsNotAuthenticatedResponse(out json.RawMessage) bool {
	if len(out) == 0 {
		return false
	}
	var response map[string]interface{}
	if err := json.Unmarshal(out, &response); err != nil {
		return false
	}
	errMap, ok := response["error"].(map[string]interface{})
	if !ok {
		return false
	}
	texts := []interface{}{errMap["message"]}
	if data, ok := errMap["data"].(map[string]interface{}); ok {
		if errname, _ := data["errname"].(string); errname == "ENOTAUTHENTICATED" {
			return true
		}
		texts = append(texts, data["reason"])
	}
	for _, t := range texts {
		if str, ok := t.(string); ok && strings.Contains(strings.ToLower(str), "not authenticated") {
			return true
		}
	}
	return false
}

// removeSession frees the slot, but only if it still belongs to the given session future
func (d *DaemonContext) removeSession(sessionKey string, channel int, future *Future[*TruenasSession]) {
	d.mapMtx.Lock()
	defer d.mapMtx.Unlock()
	sessionList, exists := d.sessionMap_[sessionKey]
	if !exists || channel < 0 || channel >= len(sessionList) || sessionList[channel] != future {
		return
	}
	sessionList[channel] = nil
	for _, f := range sessionList {
		if f != nil {
			return
		}
	}
	delete(d.sessionMap_, sessionKey)
}

func (d *DaemonContext) discardSession(s *TruenasSession, logout bool) {
	s.markClosing()
	d.removeSession(s.sessionKey, s.channel, s.future)
	s.shutdown(logout)
}

func (d *DaemonContext) reapIdleSessions() {
	if d.sessionIdleTimeout <= 0 {
		return
	}
	now := time.Now()
	idle := make([]*TruenasSession, 0)

	d.mapMtx.Lock()
	for _, futureSessions := range d.sessionMap_ {
		for _, f := range futureSessions {
			if f == nil {
				continue
			}
			done, s, err := f.Peek()
			if done && err == nil && s != nil && s.tryMarkIdleClosing(now, d.sessionIdleTimeout) {
				idle = append(idle, s)
			}
		}
	}
	d.mapMtx.Unlock()

	for _, s := range idle {
		log.Println("Daemon: closing TrueNAS session idle for more than", d.sessionIdleTimeout.String())
		d.removeSession(s.sessionKey, s.channel, s.future)
		go s.shutdown(true)
	}
}

func (d *DaemonContext) closeAllSessions() {
	sessions := make([]*TruenasSession, 0)
	d.mapMtx.Lock()
	for _, futureSessions := range d.sessionMap_ {
		for _, f := range futureSessions {
			if f == nil {
				continue
			}
			if _, s, _ := f.Peek(); s != nil {
				sessions = append(sessions, s)
			}
		}
	}
	d.sessionMap_ = make(map[string][]*Future[*TruenasSession])
	d.mapMtx.Unlock()

	var wg sync.WaitGroup
	for _, s := range sessions {
		wg.Add(1)
		go func(s *TruenasSession) {
			defer wg.Done()
			s.shutdown(true)
		}(s)
	}
	wg.Wait()
}

func (s *TruenasSession) tryReserve() bool {
	s.connMtx.Lock()
	defer s.connMtx.Unlock()
	if s.closing_ || s.inFlight_ > 0 {
		return false
	}
	s.inFlight_++
	s.lastUsed_ = time.Now()
	return true
}

func (s *TruenasSession) release() {
	s.connMtx.Lock()
	defer s.connMtx.Unlock()
	if s.inFlight_ > 0 {
		s.inFlight_--
	}
	s.lastUsed_ = time.Now()
}

func (s *TruenasSession) markClosing() {
	s.connMtx.Lock()
	s.closing_ = true
	s.connMtx.Unlock()
}

func (s *TruenasSession) tryMarkIdleClosing(now time.Time, idleTimeout time.Duration) bool {
	s.connMtx.Lock()
	defer s.connMtx.Unlock()
	if s.closing_ || s.inFlight_ > 0 || now.Sub(s.lastUsed_) < idleTimeout {
		return false
	}
	s.closing_ = true
	return true
}

// shutdown disconnects cleanly: optional auth.logout, then a WebSocket close handshake,
// so the TrueNAS middleware can drop the session instead of keeping it around.
func (s *TruenasSession) shutdown(logout bool) {
	s.markClosing()
	if logout {
		if out, err, _ := s.callJson("auth.logout", sessionLogoutTimeout, []interface{}{}); err != nil {
			log.Println("Daemon: auth.logout failed:", err)
		} else if errMsg := ExtractApiError(out); errMsg != "" && !IsNotAuthenticatedResponse(out) {
			log.Println("Daemon: auth.logout failed:", errMsg)
		}
	}
	msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
	if err := s.conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(sessionCloseGracePeriod)); err == nil {
		// give the server a moment to answer the close handshake before dropping the socket
		select {
		case <-s.doneCh:
		case <-time.After(sessionCloseGracePeriod):
		}
	}
	_ = s.conn.Close()
}

func (s *TruenasSession) keepAlive() {
	ticker := time.NewTicker(s.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.doneCh:
			return
		case <-ticker.C:
			if err := s.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.pingInterval)); err != nil {
				log.Println("Daemon: websocket ping failed:", err)
				_ = s.conn.Close()
				return
			}
		}
	}
}

func (s *TruenasSession) callJson(method string, timeoutStr string, request []interface{}) (json.RawMessage, error, bool) {
	if strings.HasPrefix(method, TNC_PREFIX_STRING) {
		out, err := s.handleDaemonProcedure(method[len(TNC_PREFIX_STRING):], timeoutStr, request)
		return out, err, false
	}

	s.connMtx.Lock()
	s.curCallId_++
	callId := s.curCallId_
	fCall := MakeFuture[json.RawMessage]()
	s.callMap_[callId] = fCall
	s.connMtx.Unlock()

	defer func() {
		s.connMtx.Lock()
		delete(s.callMap_, callId)
		s.connMtx.Unlock()
	}()

	reqMsg := make(map[string]interface{})
	reqMsg["jsonrpc"] = "2.0"
	reqMsg["method"] = method
	reqMsg["params"] = request
	reqMsg["id"] = callId

	//log.Println("Writing JSON request with callId:", callId, reqMsg)

	s.writeMtx.Lock()
	err := wrapWriteJSON(s.conn, reqMsg)
	s.writeMtx.Unlock()
	if err != nil {
		errMsg := err.Error()
		shouldRetry := strings.Contains(errMsg, "use of closed network connection") || strings.Contains(errMsg, "gorilla panic") ||
			strings.Contains(errMsg, "broken pipe") || strings.Contains(errMsg, "connection reset") || errors.Is(err, websocket.ErrCloseSent)
		return nil, err, shouldRetry
	}

	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		timeout = time.Duration(10) * time.Second
	}

	isDone, dataRes, err := AwaitFutureOrTimeout(fCall, timeout)
	if !isDone {
		timeoutParsed := timeout.String()
		return nil, fmt.Errorf("Request timed out (exceeded %s)", timeoutParsed), false
	}

	if err != nil {
		return nil, err, false
	}

	return dataRes, nil, false
}

func wrapWriteJSON(conn *websocket.Conn, reqMsg interface{}) (err error) {
	err = nil
	defer func() {
		if r := recover(); r != nil {
			msg := "gorilla panic: "
			if rErr, ok := r.(error); ok {
				innerMsg := rErr.Error()
				if innerMsg != "" {
					msg += innerMsg
				}
			} else {
				msg += fmt.Sprint(r)
			}
			err = errors.New(msg)
		}
	}()
	err = conn.WriteJSON(reqMsg)
	return err
}

func (s *TruenasSession) listen() {
	var err error
	pongSeen := false
	s.conn.SetPongHandler(func(string) error {
		pongSeen = true
		return s.conn.SetReadDeadline(time.Now().Add(s.readTimeout))
	})
	defer func() {
		if r := recover(); r != nil {
			log.Println("Recovered from panic:", r)
		}
		internalErr := fmt.Errorf("listen() exiting: %v", err)
		close(s.doneCh)
		_ = s.conn.Close()
		s.connMtx.Lock()
		s.closing_ = true
		for _, f := range s.callMap_ {
			f.Fail(internalErr)
		}
		for _, f := range s.jobMap_ {
			f.Fail(internalErr)
		}
		s.connMtx.Unlock()
		s.ctx.removeSession(s.sessionKey, s.channel, s.future)
		log.Println("listen exiting")
	}()
	for true {
		var message json.RawMessage
		_, message, err = s.conn.ReadMessage()
		if err != nil {
			log.Println("listen s.conn.ReadMessage:", err)
			return
		}
		if pongSeen {
			_ = s.conn.SetReadDeadline(time.Now().Add(s.readTimeout))
		}

		var response interface{}
		if err = json.Unmarshal(message, &response); err != nil {
			continue
		}
		responseMap, ok := response.(map[string]interface{})
		if !ok {
			continue
		}

		innerJobId := int64(-1)

		method, _ := responseMap["method"].(string)
		var fields map[string]interface{}

		if method == "collection_update" {
			params, _ := responseMap["params"].(map[string]interface{})
			jobIdF, _ := params["id"].(float64)
			fields, _ = params["fields"].(map[string]interface{})
			state, _ := fields["state"].(string)

			if state == "SUCCESS" || state == "FAILED" {
				innerJobId = int64(jobIdF)
				if innerMethod, _ := fields["method"].(string); innerMethod == JOB_WAIT_STRING {
					if args, ok := fields["arguments"].([]interface{}); ok && len(args) > 0 {
						if value, ok := args[0].(float64); ok {
							innerJobId = int64(value)
						}
					}
				}
			}
		}

		idValue := int64(-1)
		if id, exists := responseMap["id"]; exists {
			idFloat, _ := id.(float64)
			idValue = int64(idFloat)
		}

		var fJob *Future[json.RawMessage]
		var fCall *Future[json.RawMessage]
		var exists bool

		if innerJobId >= 0 || idValue >= 0 {
			s.connMtx.Lock()
			if innerJobId >= 0 {
				fJob, exists = s.jobMap_[innerJobId]
				if !exists {
					fJob = MakeFuture[json.RawMessage]()
					s.jobMap_[innerJobId] = fJob
				}
			}
			if idValue >= 0 {
				fCall, exists = s.callMap_[idValue]
			}
			s.connMtx.Unlock()
		}

		if fJob != nil && fields != nil {
			fJob.Reach(json.Marshal(fields))
		}
		if fCall != nil {
			fCall.Complete(message)
		}
	}
}

func (s *TruenasSession) handleDaemonProcedure(proc string, timeoutStr string, params []interface{}) (json.RawMessage, error) {
	isFirstParamNumber := false
	firstParamAsNumber := int64(0)
	nParams := len(params)
	if nParams > 0 {
		if n, ok := params[0].(float64); ok {
			firstParamAsNumber = int64(n)
			isFirstParamNumber = true
		}
	}

	switch proc {
	case "peek_job":
		if !isFirstParamNumber {
			return nil, fmt.Errorf("tnc_daemon.peek_job expects the first parameter to be a job number")
		}
		fJob := s.getJobFuture(firstParamAsNumber)
		if fJob == nil {
			return nil, fmt.Errorf("Job #%d could not be found internally", firstParamAsNumber)
		}
		isDone, response, err := fJob.Peek()
		if isDone {
			return response, err
		}
		return MakeIncompleteJobStatus(firstParamAsNumber)

	case "await_job":
		if !isFirstParamNumber {
			return nil, fmt.Errorf("tnc_daemon.await_job expects the first parameter to be a job number")
		}

		s.connMtx.Lock()
		fJob, exists := s.jobMap_[firstParamAsNumber]
		if !exists {
			fJob = MakeFuture[json.RawMessage]()
			s.jobMap_[firstParamAsNumber] = fJob
		}
		s.connMtx.Unlock()

		if exists {
			isDone, response, err := fJob.Peek()
			if isDone {
				return response, err
			}
		}

		out, err, _ := s.callJson(JOB_WAIT_STRING, timeoutStr, []interface{}{firstParamAsNumber})
		if err != nil {
			return nil, err
		}
		if IsNotAuthenticatedResponse(out) {
			// the job event would never arrive; let the caller reconnect and retry
			return out, nil
		}

		return fJob.Get()
	}

	return nil, fmt.Errorf("Unrecognised daemon command \"tnc_daemon.%s\"", proc)
}

func (s *TruenasSession) getJobFuture(id int64) *Future[json.RawMessage] {
	s.connMtx.Lock()
	defer s.connMtx.Unlock()
	f, exists := s.jobMap_[id]
	if !exists {
		return nil
	}
	return f
}

func (s *TruenasSession) getCallFuture(id int64) *Future[json.RawMessage] {
	s.connMtx.Lock()
	defer s.connMtx.Unlock()
	f, exists := s.callMap_[id]
	if !exists {
		return nil
	}
	return f
}

func MakeIncompleteJobStatus(jobId int64) (json.RawMessage, error) {
	params := make(map[string]interface{})
	params["arguments"] = []interface{}{jobId}
	params["state"] = "WAITING"
	return json.Marshal(params)
}
