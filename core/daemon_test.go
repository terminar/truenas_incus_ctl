package core

import (
	"encoding/json"
	"fmt"
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

	snapshots       []map[string]interface{}
	snapshotQueries []interface{} // filters received by zfs.snapshot.query
	snapshotDelay   time.Duration

	loginDelay        time.Duration
	loginsInFlight    int
	maxLoginsInFlight int
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
			f.mtx.Lock()
			f.loginsInFlight++
			if f.loginsInFlight > f.maxLoginsInFlight {
				f.maxLoginsInFlight = f.loginsInFlight
			}
			delay := f.loginDelay
			f.mtx.Unlock()
			time.Sleep(delay)
			ok := len(params) > 0 && params[0] == f.validKey
			f.mtx.Lock()
			f.loginsInFlight--
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
		case method == "zfs.snapshot.query":
			f.mtx.Lock()
			f.snapshotQueries = append(f.snapshotQueries, params[0])
			delay := f.snapshotDelay
			result := make([]map[string]interface{}, 0)
			filters, _ := params[0].([]interface{})
			for _, snap := range f.snapshots {
				// the fake only understands "no filter" and the id fast path
				if len(filters) == 0 || snap["id"] == filters[0].([]interface{})[2] {
					result = append(result, snap)
				}
			}
			f.mtx.Unlock()
			go func(resp map[string]interface{}) {
				time.Sleep(delay)
				resp["result"] = result
				fc.writeMtx.Lock()
				_ = fc.conn.WriteJSON(resp)
				fc.writeMtx.Unlock()
			}(resp)
			continue
		case method == "test.sleep":
			// answered asynchronously, like the middleware handles concurrent calls on one socket
			go func(resp map[string]interface{}) {
				if len(params) > 0 {
					if ms, ok := params[0].(float64); ok {
						time.Sleep(time.Duration(ms) * time.Millisecond)
					}
				}
				resp["result"] = params
				fc.writeMtx.Lock()
				_ = fc.conn.WriteJSON(resp)
				fc.writeMtx.Unlock()
			}(resp)
			continue
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

func (f *fakeTruenas) setLoginDelay(d time.Duration) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.loginDelay = d
}

func (f *fakeTruenas) loginConcurrency() int {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.maxLoginsInFlight
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
	d := newDaemonContext(0, sessionIdleTimeout, -1)
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
	d := newDaemonContext(100*time.Millisecond, 0, -1)

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

// A burst of parallel requests (e.g. the Incus UI querying snapshots of many instances)
// must share a few connections instead of logging in once per request.
func TestDaemonBurstSharesConnections(t *testing.T) {
	f := newFakeTruenas(t)
	f.setLoginDelay(50 * time.Millisecond)
	d := newTestDaemon(t, 0)

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := daemonCall(d, f.url(), "good-key", "test.sleep", 200)
			if err == nil && !strings.Contains(string(out), "200") {
				err = fmt.Errorf("unexpected response %s", out)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("call failed: %v", err)
		}
	}

	logins, _, _, open := f.stats()
	if logins > maxSessionsPerHost {
		t.Fatalf("%d parallel requests caused %d logins, expected at most %d", n, logins, maxSessionsPerHost)
	}
	if open > maxSessionsPerHost {
		t.Fatalf("%d connections open, expected at most %d", open, maxSessionsPerHost)
	}
	if c := f.loginConcurrency(); c > 1 {
		t.Fatalf("%d logins ran concurrently, expected them to be serialized", c)
	}
}

// When one call finds the session expired, calls still running on the same
// connection must not be cut off by the reconnect.
func TestDaemonReconnectDoesNotAbortCallsInFlight(t *testing.T) {
	f := newFakeTruenas(t)
	d := newTestDaemon(t, 0)

	if _, err := daemonCall(d, f.url(), "good-key", "test.echo", "a"); err != nil {
		t.Fatal(err)
	}

	slowDone := make(chan error, 1)
	go func() {
		out, err := daemonCall(d, f.url(), "good-key", "test.sleep", 300)
		if err == nil && !strings.Contains(string(out), "300") {
			err = fmt.Errorf("unexpected response %s", out)
		}
		slowDone <- err
	}()
	time.Sleep(50 * time.Millisecond) // slow call is now in flight

	f.expireAll()
	out, err := daemonCall(d, f.url(), "good-key", "test.echo", "b")
	if err != nil || !strings.Contains(string(out), `"b"`) {
		t.Fatalf("call after expiry failed: %v %s", err, out)
	}
	if err := <-slowDone; err != nil {
		t.Fatalf("call in flight was aborted by the reconnect: %v", err)
	}
	waitFor(t, "expired connection to be closed after draining", func() bool {
		_, _, _, open := f.stats()
		return open <= 2 && sessionCount(d) >= 1
	})
}

func (f *fakeTruenas) addSnapshots(names ...string) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	for _, name := range names {
		at := strings.Index(name, "@")
		f.snapshots = append(f.snapshots, map[string]interface{}{
			"id": name, "name": name, "dataset": name[:at], "snapshot_name": name[at+1:],
			"pool": name[:strings.Index(name, "/")], "type": "SNAPSHOT",
		})
	}
}

func (f *fakeTruenas) snapshotQueryCount() int {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return len(f.snapshotQueries)
}

var testSnapshotOptions = map[string]interface{}{"extra": map[string]interface{}{
	"flat": false, "properties": []interface{}{"name", "used", "referenced", "createtxg"}, "retrieve_children": true, "user_properties": false,
}}

// the filter truenas_incus_ctl builds for "list -r <dataset>"
func recursiveDatasetFilter(datasets ...string) []interface{} {
	operands := make([]interface{}, 0)
	for _, ds := range datasets {
		operands = append(operands, []interface{}{"dataset", "=", ds}, []interface{}{"dataset", "^", ds + "/"})
	}
	return []interface{}{[]interface{}{"OR", operands}}
}

func snapshotNames(t *testing.T, out json.RawMessage) []string {
	t.Helper()
	var response struct {
		Result []map[string]interface{} `json:"result"`
	}
	if err := json.Unmarshal(out, &response); err != nil {
		t.Fatalf("bad response %s: %v", out, err)
	}
	names := make([]string, 0)
	for _, r := range response.Result {
		names = append(names, r["name"].(string))
	}
	return names
}

func newCachingTestDaemon(t *testing.T, ttl time.Duration) *DaemonContext {
	d := newDaemonContext(0, 0, ttl)
	t.Cleanup(d.closeAllSessions)
	return d
}

func snapshotQuery(d *DaemonContext, f *fakeTruenas, filter []interface{}) (json.RawMessage, error) {
	return daemonCall(d, f.url(), "good-key", "zfs.snapshot.query", filter, testSnapshotOptions)
}

func TestSnapshotQueriesShareOneFetch(t *testing.T) {
	f := newFakeTruenas(t)
	f.addSnapshots("tank/a@s1", "tank/a@s2", "tank/a/child@s1", "tank/ab@s1", "tank/b@s1")
	f.snapshotDelay = 200 * time.Millisecond
	d := newCachingTestDaemon(t, time.Second)

	expected := map[string]string{
		"tank/a": "tank/a@s1,tank/a@s2,tank/a/child@s1", // children included, tank/ab not
		"tank/b": "tank/b@s1",
		"tank/c": "",
	}
	var wg sync.WaitGroup
	errs := make(chan error, 30)
	for i := 0; i < 30; i++ {
		ds := []string{"tank/a", "tank/b", "tank/c"}[i%3]
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := snapshotQuery(d, f, recursiveDatasetFilter(ds))
			if err == nil {
				if got := strings.Join(snapshotNames(t, out), ","); got != expected[ds] {
					err = fmt.Errorf("%s: got %q, want %q", ds, got, expected[ds])
				}
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := f.snapshotQueryCount(); n != 1 {
		t.Fatalf("expected 30 parallel snapshot queries to cause 1 fetch, got %d", n)
	}
	if filter := f.snapshotQueries[0].([]interface{}); len(filter) != 0 {
		t.Fatalf("expected the shared fetch to use no filter, got %v", filter)
	}
}

func TestSnapshotCacheIsInvalidatedByWrites(t *testing.T) {
	f := newFakeTruenas(t)
	f.addSnapshots("tank/a@s1")
	d := newCachingTestDaemon(t, time.Minute)

	for i := 0; i < 3; i++ {
		if _, err := snapshotQuery(d, f, recursiveDatasetFilter("tank/a")); err != nil {
			t.Fatal(err)
		}
	}
	if n := f.snapshotQueryCount(); n != 1 {
		t.Fatalf("expected cached results within the TTL, got %d fetches", n)
	}

	// read-only calls keep the cache
	if _, err := daemonCall(d, f.url(), "good-key", "pool.dataset.query"); err != nil {
		t.Fatal(err)
	}
	f.addSnapshots("tank/a@s2")
	if _, err := daemonCall(d, f.url(), "good-key", "zfs.snapshot.create", map[string]interface{}{"dataset": "tank/a", "name": "s2"}); err != nil {
		t.Fatal(err)
	}
	out, err := snapshotQuery(d, f, recursiveDatasetFilter("tank/a"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(snapshotNames(t, out), ","); got != "tank/a@s1,tank/a@s2" {
		t.Fatalf("stale result after a write: %q", got)
	}
	if n := f.snapshotQueryCount(); n != 2 {
		t.Fatalf("expected a new fetch after the write, got %d fetches", n)
	}
}

func TestSnapshotFetchRunningDuringWriteIsNotReused(t *testing.T) {
	f := newFakeTruenas(t)
	f.addSnapshots("tank/a@s1")
	f.snapshotDelay = 300 * time.Millisecond
	d := newCachingTestDaemon(t, time.Minute)

	first := make(chan error, 1)
	go func() {
		_, err := snapshotQuery(d, f, recursiveDatasetFilter("tank/a"))
		first <- err
	}()
	waitFor(t, "first fetch to start", func() bool { return f.snapshotQueryCount() == 1 })

	if _, err := daemonCall(d, f.url(), "good-key", "zfs.snapshot.delete", "tank/a@s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshotQuery(d, f, recursiveDatasetFilter("tank/a")); err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if n := f.snapshotQueryCount(); n != 2 {
		t.Fatalf("a query after a write must not join a fetch started before it, got %d fetches", n)
	}
}

func TestSnapshotCacheExpires(t *testing.T) {
	f := newFakeTruenas(t)
	f.addSnapshots("tank/a@s1")
	d := newCachingTestDaemon(t, 50*time.Millisecond)

	if _, err := snapshotQuery(d, f, recursiveDatasetFilter("tank/a")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, err := snapshotQuery(d, f, recursiveDatasetFilter("tank/a")); err != nil {
		t.Fatal(err)
	}
	if n := f.snapshotQueryCount(); n != 2 {
		t.Fatalf("expected a new fetch after the TTL, got %d fetches", n)
	}
}

func TestSnapshotQueriesBypassingTheCache(t *testing.T) {
	f := newFakeTruenas(t)
	f.addSnapshots("tank/a@s1", "tank/a@s2")
	d := newCachingTestDaemon(t, time.Minute)

	// the middleware's own fast path for a single snapshot is cheaper than fetching everything
	out, err := snapshotQuery(d, f, []interface{}{[]interface{}{"id", "=", "tank/a@s2"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(snapshotNames(t, out), ","); got != "tank/a@s2" {
		t.Fatalf("got %q", got)
	}
	if filter := f.snapshotQueries[0].([]interface{}); len(filter) != 1 {
		t.Fatalf("expected the id lookup to be passed through, got %v", filter)
	}

	// options the daemon doesn't handle itself
	if _, err := daemonCall(d, f.url(), "good-key", "zfs.snapshot.query", []interface{}{}, map[string]interface{}{"count": true}); err != nil {
		t.Fatal(err)
	}
	if n := f.snapshotQueryCount(); n != 2 {
		t.Fatalf("expected both queries to reach the middleware, got %d", n)
	}
}

func TestSnapshotQueryCacheKey(t *testing.T) {
	opts := map[string]interface{}{"extra": map[string]interface{}{"properties": []interface{}{"used"}}}
	cases := []struct {
		name    string
		filters []interface{}
		options map[string]interface{}
		ok      bool
	}{
		{"recursive datasets", recursiveDatasetFilter("tank/a", "tank/b"), opts, true},
		{"no filter", []interface{}{}, opts, true},
		{"dataset in", []interface{}{[]interface{}{"dataset", "in", []interface{}{"tank/a"}}}, opts, true},
		{"AND inside OR", []interface{}{[]interface{}{"OR", []interface{}{
			[]interface{}{[]interface{}{"pool", "=", "tank"}, []interface{}{"snapshot_name", "^", "auto-"}},
			[]interface{}{"name", "$", "@keep"},
		}}}, opts, true},
		{"id fast path", []interface{}{[]interface{}{"id", "=", "tank/a@s"}}, opts, false},
		{"property filter", []interface{}{[]interface{}{"properties.used.parsed", ">", 0}}, opts, false},
		{"unknown operator", []interface{}{[]interface{}{"name", "~", "x"}}, opts, false},
		{"select", []interface{}{}, map[string]interface{}{"select": []interface{}{"name"}}, false},
		{"retention", []interface{}{}, map[string]interface{}{"extra": map[string]interface{}{"retention": true}}, false},
	}
	for _, c := range cases {
		if _, _, ok := snapshotQueryCacheKey("zfs.snapshot.query", []interface{}{c.filters, c.options}); ok != c.ok {
			t.Errorf("%s: cacheable = %v, want %v", c.name, ok, c.ok)
		}
	}
	if _, _, ok := snapshotQueryCacheKey("pool.dataset.query", []interface{}{[]interface{}{}, opts}); ok {
		t.Errorf("only zfs.snapshot.query may be cached")
	}
}

func TestSnapshotFilterMatching(t *testing.T) {
	all := []map[string]interface{}{
		{"name": "tank/a@auto-1", "dataset": "tank/a", "pool": "tank", "snapshot_name": "auto-1"},
		{"name": "tank/a@keep", "dataset": "tank/a", "pool": "tank", "snapshot_name": "keep"},
		{"name": "tank/b@auto-1", "dataset": "tank/b", "pool": "tank", "snapshot_name": "auto-1"},
		{"name": "zdata/c@auto-1", "dataset": "zdata/c", "pool": "zdata", "snapshot_name": "auto-1"},
	}
	cases := map[string][]interface{}{
		"tank/a@auto-1,tank/a@keep,tank/b@auto-1": {[]interface{}{"pool", "=", "tank"}},
		"tank/a@auto-1,tank/b@auto-1":             {[]interface{}{"pool", "=", "tank"}, []interface{}{"snapshot_name", "^", "auto-"}},
		"tank/a@keep,zdata/c@auto-1": {[]interface{}{"OR", []interface{}{
			[]interface{}{"name", "$", "@keep"},
			[]interface{}{[]interface{}{"pool", "!=", "tank"}, []interface{}{"dataset", "nin", []interface{}{"tank/a"}}},
		}}},
		"tank/b@auto-1,zdata/c@auto-1": {[]interface{}{"dataset", "in", []interface{}{"tank/b", "zdata/c"}}},
	}
	for want, filters := range cases {
		names := make([]string, 0)
		for _, s := range filterSnapshots(all, filters) {
			names = append(names, s["name"].(string))
		}
		if got := strings.Join(names, ","); got != want {
			t.Errorf("filter %v: got %q, want %q", filters, got, want)
		}
	}
}
