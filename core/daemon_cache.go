package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// The daemon shares zfs.snapshot.query calls between concurrent requests and keeps the result for
// a short time. Any call that might change state empties the cache. Two modes:
//
//   - "query" (default): identical queries (same filters and options) are sent once. Meant for a
//     middleware with patches/middlewared-25.10-snapshot-query-scope.patch, which only reads the
//     snapshots of the filtered datasets (~0.01s for one dataset instead of ~1s).
//   - "all": an unpatched TrueNAS 25.10 middleware answers every zfs.snapshot.query that isn't a
//     plain id/name lookup by reading *all* snapshots with their properties and filtering
//     afterwards (~1s per query with ~2400 snapshots). In this mode the daemon fetches all
//     snapshots once and filters per request itself.

const SNAPSHOT_QUERY_METHOD = "zfs.snapshot.query"
const DEFAULT_SNAPSHOT_CACHE_TTL = 5 * time.Second

const SNAPSHOT_CACHE_MODE_QUERY = "query"
const SNAPSHOT_CACHE_MODE_ALL = "all"
const DEFAULT_SNAPSHOT_CACHE_MODE = SNAPSHOT_CACHE_MODE_QUERY

func IsValidSnapshotCacheMode(mode string) bool {
	return mode == SNAPSHOT_CACHE_MODE_QUERY || mode == SNAPSHOT_CACHE_MODE_ALL
}

type snapshotCache struct {
	mtx        sync.Mutex
	ttl        time.Duration
	generation uint64
	entries    map[string]*snapshotCacheEntry
}

type snapshotCacheEntry struct {
	generation uint64
	done       bool
	fetchedAt  time.Time
	future     *Future[[]map[string]interface{}]
}

func newSnapshotCache(ttl time.Duration) *snapshotCache {
	return &snapshotCache{
		ttl:     ttl,
		entries: make(map[string]*snapshotCacheEntry),
	}
}

// invalidate drops all cached results; fetches already running won't be stored or shared anymore
func (c *snapshotCache) invalidate() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.generation++
	c.entries = make(map[string]*snapshotCacheEntry)
}

// get returns the snapshots for cacheKey, either cached, from a fetch already running, or by
// calling fetch, which must return the raw JSON-RPC response.
func (c *snapshotCache) get(cacheKey string, fetch func() (json.RawMessage, error)) ([]map[string]interface{}, error) {
	c.mtx.Lock()
	e := c.entries[cacheKey]
	if e != nil && e.generation == c.generation && (!e.done || time.Since(e.fetchedAt) < c.ttl) {
		c.mtx.Unlock()
		return e.future.Get()
	}
	e = &snapshotCacheEntry{
		generation: c.generation,
		future:     MakeFuture[[]map[string]interface{}](),
	}
	c.entries[cacheKey] = e
	c.mtx.Unlock()

	results, err := fetchAllSnapshots(fetch)

	c.mtx.Lock()
	if err != nil || c.ttl <= 0 || e.generation != c.generation {
		if c.entries[cacheKey] == e {
			delete(c.entries, cacheKey)
		}
	} else {
		e.done = true
		e.fetchedAt = time.Now()
	}
	c.mtx.Unlock()

	e.future.Reach(results, err)
	return results, err
}

func fetchAllSnapshots(fetch func() (json.RawMessage, error)) ([]map[string]interface{}, error) {
	out, err := fetch()
	if err != nil {
		return nil, err
	}
	if errMsg := ExtractApiError(out); errMsg != "" {
		return nil, fmt.Errorf("%s", errMsg)
	}
	var response struct {
		Result []map[string]interface{} `json:"result"`
	}
	if err := json.Unmarshal(out, &response); err != nil {
		return nil, fmt.Errorf("Could not parse %s response: %w", SNAPSHOT_QUERY_METHOD, err)
	}
	if response.Result == nil {
		return nil, fmt.Errorf("Unexpected %s response: %s", SNAPSHOT_QUERY_METHOD, string(out))
	}
	return response.Result, nil
}

// snapshotQueryCacheKey decides whether a zfs.snapshot.query can be answered from the cache.
// It returns the filter to apply locally and the key under which all snapshots are cached
// (the query options, since they decide which properties are returned).
func snapshotQueryCacheKey(method string, params []interface{}) ([]interface{}, string, bool) {
	if method != SNAPSHOT_QUERY_METHOD || len(params) != 2 {
		return nil, "", false
	}
	filters, ok := params[0].([]interface{})
	if !ok || !isSupportedFilterList(filters) || isMiddlewareFastPath(filters) {
		return nil, "", false
	}
	options, ok := params[1].(map[string]interface{})
	if !ok {
		return nil, "", false
	}
	for k := range options {
		// select, count, limit, order_by etc. are left to the middleware
		if k != "extra" {
			return nil, "", false
		}
	}
	if extra, ok := options["extra"].(map[string]interface{}); ok && IsValueTrue(extra, "retention") {
		return nil, "", false
	}
	key, err := json.Marshal(options) // map keys are sorted, so equal options give equal keys
	if err != nil {
		return nil, "", false
	}
	return filters, string(key), true
}

// A single id/name equality filter makes the middleware read just that snapshot
func isMiddlewareFastPath(filters []interface{}) bool {
	if len(filters) != 1 {
		return false
	}
	f, ok := filters[0].([]interface{})
	if !ok || len(f) != 3 {
		return false
	}
	return (f[0] == "id" || f[0] == "name") && f[1] == "="
}

var cacheFilterFields = map[string]bool{"id": true, "name": true, "dataset": true, "pool": true, "snapshot_name": true}

// filter semantics follow middlewared's filter_list: a list of filters is an AND,
// ["OR", [operand, ...]] is an OR, and an operand is a filter or a list of filters (AND)
func isSupportedFilterList(filters []interface{}) bool {
	for _, f := range filters {
		if !isSupportedFilter(f) {
			return false
		}
	}
	return true
}

func isSupportedFilter(value interface{}) bool {
	f, ok := value.([]interface{})
	if !ok {
		return false
	}
	if len(f) == 2 && f[0] == "OR" {
		operands, ok := f[1].([]interface{})
		if !ok || len(operands) == 0 {
			return false
		}
		for _, o := range operands {
			if !isSupportedOperand(o) {
				return false
			}
		}
		return true
	}
	if len(f) != 3 {
		return false
	}
	field, _ := f[0].(string)
	op, _ := f[1].(string)
	if !cacheFilterFields[field] {
		return false
	}
	switch op {
	case "=", "!=", "^", "$":
		_, ok := f[2].(string)
		return ok
	case "in", "nin":
		list, ok := f[2].([]interface{})
		if !ok {
			return false
		}
		for _, v := range list {
			if _, ok := v.(string); !ok {
				return false
			}
		}
		return true
	}
	return false
}

func isSupportedOperand(o interface{}) bool {
	list, ok := o.([]interface{})
	if !ok || len(list) == 0 {
		return false
	}
	if _, isString := list[0].(string); isString {
		return isSupportedFilter(list)
	}
	return isSupportedFilterList(list)
}

func matchesFilterList(item map[string]interface{}, filters []interface{}) bool {
	for _, f := range filters {
		if !matchesFilter(item, f.([]interface{})) {
			return false
		}
	}
	return true
}

func matchesFilter(item map[string]interface{}, f []interface{}) bool {
	if len(f) == 2 && f[0] == "OR" {
		for _, o := range f[1].([]interface{}) {
			list := o.([]interface{})
			if _, isString := list[0].(string); isString {
				if matchesFilter(item, list) {
					return true
				}
			} else if matchesFilterList(item, list) {
				return true
			}
		}
		return false
	}
	value, _ := item[f[0].(string)].(string)
	switch f[1].(string) {
	case "=":
		return value == f[2].(string)
	case "!=":
		return value != f[2].(string)
	case "^":
		return strings.HasPrefix(value, f[2].(string))
	case "$":
		return strings.HasSuffix(value, f[2].(string))
	case "in", "nin":
		found := false
		for _, v := range f[2].([]interface{}) {
			if value == v.(string) {
				found = true
				break
			}
		}
		return found == (f[1].(string) == "in")
	}
	return false
}

func filterSnapshots(all []map[string]interface{}, filters []interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0)
	for _, item := range all {
		if matchesFilterList(item, filters) {
			out = append(out, item)
		}
	}
	return out
}

func makeResultResponse(result interface{}) (json.RawMessage, error) {
	return json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "result": result})
}

// isReadOnlyMethod reports calls that can't change snapshots; anything else empties the cache
func isReadOnlyMethod(method string) bool {
	if strings.HasSuffix(method, ".query") || strings.HasSuffix(method, ".get_instance") || strings.HasSuffix(method, ".config") {
		return true
	}
	switch method {
	case "core.ping", "core.get_jobs", "auth.me", "system.info", "system.version",
		TNC_PREFIX_STRING + "ping", TNC_PREFIX_STRING + "peek_job":
		return true
	}
	return false
}
