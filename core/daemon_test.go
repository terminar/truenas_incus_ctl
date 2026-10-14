package core

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func sessionCount(d *DaemonContext) int {
	d.mapMtx.Lock()
	defer d.mapMtx.Unlock()
	n := 0
	for _, list := range d.sessionMap_ {
		for _, f := range list {
			if f != nil {
				n++
			}
		}
	}
	return n
}

// fakeTruenas is a minimal JSON-RPC 2.0 over WebSocket server that mimics the
// parts of the TrueNAS middleware the daemon relies on.
type fakeTruenas struct {
	t        *testing.T
	server   *httptest.Server
	mtx      sync.Mutex
	logins   int
	logouts  int
	closes   int // clean close frames received
	open     int // currently open connections
	conns    []*fakeConn
	validKey string
	freezeCh chan struct{}
}

type fakeConn struct {
	conn          *websocket.Conn
	authenticated bool
	frozen        bool
	writeMtx      sync.Mutex
}

func newFakeTruenas(t *testing.T) *fakeTruenas {
	f := &fakeTruenas{t: t, validKey: "good-key", freezeCh: make(chan struct{})}
	upgrader := websocket.Upgrader{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fc := &fakeConn{conn: c}
		f.mtx.Lock()
		f.open++
		f.conns = append(f.conns, fc)
		f.mtx.Unlock()
		go f.serve(fc)
	}))
	t.Cleanup(func() {
		close(f.freezeCh)
		f.server.Close()
	})
	return f
}

func (f *fakeTruenas) url() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + "/api/current"
}

func (f *fakeTruenas) serve(fc *fakeConn) {
	defer func() {
		f.mtx.Lock()
		f.open--
		f.mtx.Unlock()
		fc.conn.Close()
	}()
	for {
		_, data, err := fc.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				f.mtx.Lock()
				f.closes++
				f.mtx.Unlock()
			}
			return
		}
		var req map[string]interface{}
		if json.Unmarshal(data, &req) != nil {
			continue
		}
		method, _ := req["method"].(string)
		params, _ := req["params"].([]interface{})
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req["id"]}

		f.mtx.Lock()
		frozen := fc.frozen
		authenticated := fc.authenticated
		f.mtx.Unlock()
		if frozen {
			continue
		}

		switch {
		case method == "auth.login_with_api_key":
			ok := len(params) > 0 && params[0] == f.validKey
			f.mtx.Lock()
			fc.authenticated = ok
			if ok {
				f.logins++
			}
			f.mtx.Unlock()
			resp["result"] = ok
		case method == "core.ping":
			resp["result"] = "pong"
		case !authenticated:
			resp["error"] = map[string]interface{}{
				"code":    -32001,
				"message": "Method call error",
				"data": map[string]interface{}{
					"error":   13,
					"errname": "ENOTAUTHENTICATED",
					"reason":  "Not authenticated",
				},
			}
		case method == "auth.logout":
			f.mtx.Lock()
			fc.authenticated = false
			f.logouts++
			f.mtx.Unlock()
			resp["result"] = true
		case method == "test.freeze":
			// stop reading (and therefore answering pings) like a hung peer
			<-f.freezeCh
			return
		case method == "core.subscribe":
			resp["result"] = "sub-1"
		default:
			resp["result"] = params
		}
		fc.writeMtx.Lock()
		_ = fc.conn.WriteJSON(resp)
		fc.writeMtx.Unlock()
	}
}

// expireAll simulates the middleware dropping authentication on open sockets
func (f *fakeTruenas) expireAll() {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	for _, fc := range f.conns {
		fc.authenticated = false
	}
}

func (f *fakeTruenas) stats() (logins, logouts, closes, open int) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.logins, f.logouts, f.closes, f.open
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func newTestDaemon(t *testing.T, sessionIdleTimeout time.Duration) *DaemonContext {
	d := newDaemonContext(0, sessionIdleTimeout)
	t.Cleanup(d.closeAllSessions)
	return d
}

func daemonCall(d *DaemonContext, host, key, method string, params ...interface{}) (json.RawMessage, error) {
	return daemonCallWithTimeout(d, host, key, method, "2s", params...)
}

func daemonCallWithTimeout(d *DaemonContext, host, key, method, timeout string, params ...interface{}) (json.RawMessage, error) {
	if params == nil {
		params = []interface{}{}
	}
	body, _ := json.Marshal(params)
	r := httptest.NewRequest("POST", "http://unix/tnc-daemon", strings.NewReader(string(body)))
	r.Header.Set("TNC-Host-Url", host)
	r.Header.Set("TNC-Api-Key", key)
	r.Header.Set("TNC-Call-Method", method)
	r.Header.Set("TNC-Timeout", timeout)
	return d.serveImpl(r)
}

func TestDaemonReconnectsWhenSessionIsNoLongerAuthenticated(t *testing.T) {
	f := newFakeTruenas(t)
	d := newTestDaemon(t, 0)

	out, err := daemonCall(d, f.url(), "good-key", "test.echo", "a")
	if err != nil || !strings.Contains(string(out), `"a"`) {
		t.Fatalf("first call failed: %v %s", err, out)
	}

	f.expireAll()

	out, err = daemonCall(d, f.url(), "good-key", "test.echo", "b")
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if strings.Contains(string(out), "ENOTAUTHENTICATED") || !strings.Contains(string(out), `"b"`) {
		t.Fatalf("expected the daemon to re-login transparently, got %s", out)
	}
	if logins, _, _, _ := f.stats(); logins != 2 {
		t.Fatalf("expected 2 logins, got %d", logins)
	}
	waitFor(t, "stale connection to be closed", func() bool {
		_, _, _, open := f.stats()
		return open == 1
	})
}

func TestDaemonClosesConnectionWhenLoginFails(t *testing.T) {
	f := newFakeTruenas(t)
	d := newTestDaemon(t, 0)

	_, err := daemonCall(d, f.url(), "bad-key", "test.echo", "a")
	if err == nil {
		t.Fatalf("expected login failure to be reported as an error")
	}
	waitFor(t, "connection with failed login to be closed", func() bool {
		_, _, _, open := f.stats()
		return open == 0
	})
}

func TestDaemonLogsOutWhenClosingSessions(t *testing.T) {
	f := newFakeTruenas(t)
	d := newTestDaemon(t, 0)

	if _, err := daemonCall(d, f.url(), "good-key", "test.echo", "a"); err != nil {
		t.Fatal(err)
	}
	d.closeAllSessions()

	waitFor(t, "logout and clean close", func() bool {
		_, logouts, closes, open := f.stats()
		return logouts == 1 && closes == 1 && open == 0
	})
	if n := sessionCount(d); n != 0 {
		t.Fatalf("expected no sessions left, got %d", n)
	}
}

func TestDaemonClosesIdleSessions(t *testing.T) {
	f := newFakeTruenas(t)
	d := newTestDaemon(t, 100*time.Millisecond)

	if _, err := daemonCall(d, f.url(), "good-key", "test.echo", "a"); err != nil {
		t.Fatal(err)
	}
	d.reapIdleSessions()
	if _, _, _, open := f.stats(); open != 1 || sessionCount(d) != 1 {
		t.Fatalf("session must not be reaped before the idle timeout")
	}

	time.Sleep(150 * time.Millisecond)
	d.reapIdleSessions()

	waitFor(t, "idle session to log out and close", func() bool {
		_, logouts, closes, open := f.stats()
		return logouts == 1 && closes == 1 && open == 0
	})
	if n := sessionCount(d); n != 0 {
		t.Fatalf("expected no sessions left, got %d", n)
	}

	// the next call transparently opens a new connection
	if _, err := daemonCall(d, f.url(), "good-key", "test.echo", "b"); err != nil {
		t.Fatal(err)
	}
	if logins, _, _, _ := f.stats(); logins != 2 {
		t.Fatalf("expected 2 logins, got %d", logins)
	}
}

func TestDaemonDoesNotReapBusySessions(t *testing.T) {
	f := newFakeTruenas(t)
	d := newTestDaemon(t, time.Millisecond)

	s, err := d.acquireSession(f.url()+"good-keyfalse", LoginInfo{
		call:      CallInfo{method: "auth.login_with_api_key", params: []interface{}{"good-key"}},
		serverUrl: f.url(),
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	d.reapIdleSessions()
	if sessionCount(d) != 1 {
		t.Fatalf("a session with a call in flight must not be reaped")
	}
	s.release()
	time.Sleep(10 * time.Millisecond)
	d.reapIdleSessions()
	if sessionCount(d) != 0 {
		t.Fatalf("released session should be reaped")
	}
}

func TestDaemonDropsConnectionsThatStopAnsweringPings(t *testing.T) {
	oldPing, oldRead := sessionPingInterval, sessionReadTimeout
	sessionPingInterval, sessionReadTimeout = 30*time.Millisecond, 150*time.Millisecond
	t.Cleanup(func() { sessionPingInterval, sessionReadTimeout = oldPing, oldRead })

	f := newFakeTruenas(t)
	d := newTestDaemon(t, 0)

	if _, err := daemonCall(d, f.url(), "good-key", "test.echo", "a"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // let at least one ping/pong happen

	if _, err := daemonCallWithTimeout(d, f.url(), "good-key", "test.freeze", "300ms"); err == nil {
		t.Fatalf("expected the call to the frozen server to time out")
	}
	waitFor(t, "dead connection to be dropped", func() bool { return sessionCount(d) == 0 })

	if _, err := daemonCall(d, f.url(), "good-key", "test.echo", "b"); err != nil {
		t.Fatal(err)
	}
	if logins, _, _, _ := f.stats(); logins != 2 {
		t.Fatalf("expected 2 logins, got %d", logins)
	}
}

func TestDaemonTimeoutIgnoresTimeWithRequestsInFlight(t *testing.T) {
	d := newDaemonContext(100*time.Millisecond, 0)

	d.requestStarted()
	select {
	case <-d.timeoutTimer.C:
		t.Fatalf("daemon timed out while a request was in flight")
	case <-time.After(200 * time.Millisecond):
	}
	d.requestFinished()

	select {
	case <-d.timeoutTimer.C:
	case <-time.After(time.Second):
		t.Fatalf("daemon did not time out after the last request finished")
	}
}

func TestIsNotAuthenticatedResponse(t *testing.T) {
	cases := map[string]bool{
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"Method call error","data":{"errname":"ENOTAUTHENTICATED"}}}`: true,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"Not authenticated"}}`:                                        true,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"Method call error","data":{"errname":"ENOENT"}}}`:            false,
		`{"jsonrpc":"2.0","id":1,"result":"not authenticated"}`:                                                                 false,
		``: false,
	}
	for input, expected := range cases {
		if got := IsNotAuthenticatedResponse(json.RawMessage(input)); got != expected {
			t.Errorf("IsNotAuthenticatedResponse(%s) = %v, want %v", input, got, expected)
		}
	}
}
