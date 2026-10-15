package cmd

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"truenas/truenas_incus_ctl/core"
)

func recursiveFilterFor(t *testing.T, datasets ...string) []interface{} {
	types := make([]string, len(datasets))
	for i := range types {
		types[i] = "dataset"
	}
	filter, err := makeQueryFilter(datasets, types, typeQueryParams{shouldRecurse: true})
	if err != nil {
		t.Fatal(err)
	}
	return filter
}

func TestGetFilterOrDepth(t *testing.T) {
	cases := map[int][]interface{}{
		0: {[]interface{}{"name", "in", []interface{}{"a", "b", "c"}}},
		1: recursiveFilterFor(t, "tank/a"),
		3: recursiveFilterFor(t, "tank/a", "tank/b"),
		5: recursiveFilterFor(t, "tank/a", "tank/b", "tank/c"),
	}
	for want, filter := range cases {
		if got := getFilterOrDepth(filter); got != want {
			t.Errorf("depth of %v: got %d, want %d", filter, got, want)
		}
	}
	conjunction := []interface{}{[]interface{}{"OR", []interface{}{
		[]interface{}{[]interface{}{"pool", "=", "tank"}, []interface{}{"OR", []interface{}{[]interface{}{"a", "=", "1"}, []interface{}{"a", "=", "2"}}}},
		[]interface{}{"name", "=", "x"},
	}}}
	if got := getFilterOrDepth(conjunction); got != 2 {
		t.Errorf("depth with conjunction: got %d, want 2", got)
	}
}

// fakeQuerySession answers zfs.snapshot.query calls for a single recursive dataset
// and records how many ran at the same time
type fakeQuerySession struct {
	UnitTestSession
	concurrent bool
	mtx        sync.Mutex
	filters    []string
	running    int
	maxRunning int
}

func (s *fakeQuerySession) SupportsConcurrentCalls() bool { return s.concurrent }

func (s *fakeQuerySession) CallRaw(method string, timeoutSeconds int64, params interface{}) (json.RawMessage, error) {
	data, _ := json.Marshal(params.([]interface{})[0])
	var filter []interface{}
	_ = json.Unmarshal(data, &filter)
	if getFilterOrDepth(filter) > maxQueryFilterOrDepth {
		return nil, fmt.Errorf("filter too deep for TrueNAS: %s", data)
	}
	dataset := filter[0].([]interface{})[1].([]interface{})[0].([]interface{})[2].(string)

	s.mtx.Lock()
	s.filters = append(s.filters, string(data))
	s.running++
	s.maxRunning = max(s.maxRunning, s.running)
	s.mtx.Unlock()
	time.Sleep(50 * time.Millisecond)
	s.mtx.Lock()
	s.running--
	s.mtx.Unlock()

	return json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "result": []interface{}{
		map[string]interface{}{"id": dataset + "@s1", "name": dataset + "@s1"},
		map[string]interface{}{"id": dataset + "/child@s1", "name": dataset + "/child@s1"},
	}})
}

func TestQueryApiSplitsDeepFilters(t *testing.T) {
	datasets := []string{"tank/a", "tank/b", "tank/c", "tank/d"}
	types := []string{"dataset", "dataset", "dataset", "dataset"}
	params := typeQueryParams{shouldRecurse: true, shouldSkipKeyBuild: true, valueOrder: BuildValueOrder(true)}

	for _, concurrent := range []bool{false, true} {
		s := &fakeQuerySession{concurrent: concurrent}
		var api core.Session = s
		response, err := QueryApi(api, "zfs.snapshot", datasets, types, []string{"name"}, params)
		if err != nil {
			t.Fatalf("concurrent=%v: %v", concurrent, err)
		}
		if len(s.filters) != 4 {
			t.Fatalf("concurrent=%v: expected one query per dataset, got %d: %v", concurrent, len(s.filters), s.filters)
		}
		keys := make([]string, 0)
		for k := range response.resultsMap {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		want := "tank/a/child@s1,tank/a@s1,tank/b/child@s1,tank/b@s1,tank/c/child@s1,tank/c@s1,tank/d/child@s1,tank/d@s1"
		if got := strings.Join(keys, ","); got != want {
			t.Fatalf("concurrent=%v: got %s", concurrent, got)
		}
		if len(response.strKeys) != 0 || len(response.intKeys) != 0 {
			t.Fatalf("concurrent=%v: keys must not be built when shouldSkipKeyBuild is set", concurrent)
		}
		if concurrent && s.maxRunning < 2 {
			t.Fatalf("expected the split queries to run in parallel, max running = %d", s.maxRunning)
		}
		if !concurrent && s.maxRunning != 1 {
			t.Fatalf("expected sequential queries without concurrency support, max running = %d", s.maxRunning)
		}
	}
}

func TestSplitQueryEntries(t *testing.T) {
	groups := splitQueryEntries(
		[]string{"tank/a", "tank/x@s", "tank/b", "tank/y@s"},
		[]string{"dataset", "name", "dataset", "name"},
		typeQueryParams{shouldRecurse: true})
	got := fmt.Sprint(groups)
	want := "[{[tank/a] [dataset]} {[tank/x@s tank/y@s] [name name]} {[tank/b] [dataset]}]"
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}
