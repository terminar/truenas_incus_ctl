package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDaemonSettings(t *testing.T) {
	cases := []struct {
		name   string
		config string
		want   daemonSettings
	}{
		{"all set", `{"hosts":{},"daemon":{"max_connections":3,"max_calls":6,"snapshot_cache_ttl":"10s"}}`,
			daemonSettings{maxConnections: 3, maxCalls: 6, snapshotCacheTtl: "10s"}},
		{"partially set", `{"hosts":{},"daemon":{"max_calls":2}}`, daemonSettings{maxCalls: 2}},
		{"no daemon section", `{"hosts":{}}`, daemonSettings{}},
		{"invalid values are ignored", `{"daemon":{"max_connections":0,"max_calls":2.5,"snapshot_cache_ttl":"soon"}}`, daemonSettings{}},
		{"cache disabled", `{"daemon":{"snapshot_cache_ttl":"-1s"}}`, daemonSettings{snapshotCacheTtl: "-1s"}},
	}
	for _, c := range cases {
		fileName := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(fileName, []byte(c.config), 0600); err != nil {
			t.Fatal(err)
		}
		if got := loadDaemonSettings(fileName); got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}
