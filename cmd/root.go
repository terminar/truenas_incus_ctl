package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"
	"truenas/truenas_incus_ctl/core"

	"github.com/spf13/cobra"
)

const USE_DAEMON = true

var rootCmd = &cobra.Command{
	Use: "truenas_incus_ctl",
}

var daemonCmd = &cobra.Command{
	Use:  "daemon",
	Args: cobra.MinimumNArgs(1),
	Run:  runDaemon,
}

var g_debug bool
var g_allowInsecure bool
var g_daemonSocketOverride string
var g_configFileName string
var g_configName string
var g_hostName string
var g_apiKey string
var g_daemonMaxConnections int
var g_daemonMaxCalls int
var g_daemonSnapshotCacheTtl string

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().BoolVar(&g_debug, "debug", false, "Enable debug logs")
	rootCmd.PersistentFlags().BoolVar(&g_allowInsecure, "allow-insecure", false, "Allow self-signed or non-trusted SSL certificates")
	rootCmd.PersistentFlags().StringVar(&g_daemonSocketOverride, "daemon-socket", "", "Override the default daemon socket path (~/tncdaemon.sock)")
	rootCmd.PersistentFlags().StringVarP(&g_configFileName, "config-file", "F", "", "Override config filename (~/.truenas_incus_ctl/config.json)")
	rootCmd.PersistentFlags().StringVarP(&g_configName, "config", "C", "", "Name of config to look up in config.json, defaults to first entry")
	rootCmd.PersistentFlags().StringVarP(&g_hostName, "host", "H", "", "Server hostname or ip with optional port or URL")
	rootCmd.PersistentFlags().StringVarP(&g_apiKey, "api-key", "K", "", "API key")

	daemonCmd.Flags().StringP("timeout", "t", "", "Exit the daemon if no communication occurs after this duration")
	daemonCmd.Flags().StringP("session-idle-timeout", "i", "", "Log out and close TrueNAS connections that were idle for this duration (default 2m, 0 disables)")
	daemonCmd.Flags().Int("max-connections", core.DEFAULT_MAX_SESSIONS_PER_HOST, "Maximum number of connections (and logins) per TrueNAS host")
	daemonCmd.Flags().Int("max-calls", core.DEFAULT_MAX_CALLS_PER_SESSION, "Maximum number of parallel calls per connection; further calls wait for a free slot")
	daemonCmd.Flags().String("snapshot-cache-ttl", "", "Fetch all snapshots once for concurrent zfs.snapshot.query calls and keep them for this duration (default 5s, 0 only shares running fetches, negative disables)")

	rootCmd.AddCommand(daemonCmd)
}

func RemoveGlobalFlags(flags map[string]string) {
	core.DeleteSnakeKebab(flags, "debug")
	core.DeleteSnakeKebab(flags, "allow-insecure")
	core.DeleteSnakeKebab(flags, "daemon-socket")
	core.DeleteSnakeKebab(flags, "config-file")
	core.DeleteSnakeKebab(flags, "config")
	core.DeleteSnakeKebab(flags, "host")
	core.DeleteSnakeKebab(flags, "api-key")
}

func runDaemon(cmd *cobra.Command, args []string) {
	var globalTimeoutStr string
	f := cmd.Flags().Lookup("timeout")
	if f != nil {
		globalTimeoutStr = f.Value.String()
	}
	var sessionIdleTimeoutStr string
	if f := cmd.Flags().Lookup("session-idle-timeout"); f != nil {
		sessionIdleTimeoutStr = f.Value.String()
	}
	maxConnections, _ := cmd.Flags().GetInt("max-connections")
	maxCalls, _ := cmd.Flags().GetInt("max-calls")
	snapshotCacheTtlStr, _ := cmd.Flags().GetString("snapshot-cache-ttl")
	serverSockAddr := args[0]
	if serverSockAddr == "" {
		log.Fatal("Error: path to server socket was not provided")
	}
	core.RunDaemon(serverSockAddr, globalTimeoutStr, sessionIdleTimeoutStr, maxConnections, maxCalls, snapshotCacheTtlStr)
}

func InitializeApiClient() core.Session {
	var api core.Session
	if g_hostName == "" || g_apiKey == "" {
		host, key, config, err := findCredsFromConfig(g_configFileName, g_configName, g_hostName, g_apiKey)
		if err != nil {
			log.Fatal(fmt.Errorf("Failed to parse config: %v", err))
		}
		g_hostName = host
		g_apiKey = key
		if _, exists := config["debug"]; exists {
			g_debug = core.IsValueTrue(config, "debug")
		}
		if _, exists := config["allow_insecure"]; exists {
			g_allowInsecure = core.IsValueTrue(config, "allow_insecure")
		}
		if obj, exists := config["daemon_socket"]; exists {
			g_daemonSocketOverride, _ = obj.(string)
		}
	}
	if USE_DAEMON {
		applyDaemonSettings(loadDaemonSettings(g_configFileName))
		socketPath := g_daemonSocketOverride
		if socketPath == "" {
			p, err := getHomeDirWithFallback()
			if err != nil {
				log.Fatal(err)
			}
			socketPath = path.Join(p, "tncdaemon.sock")
		}
		api = &core.ClientSession{
			HostName:      g_hostName,
			ApiKey:        g_apiKey,
			SocketPath:    socketPath,
			IsDebug:       g_debug,
			AllowInsecure: g_allowInsecure,

			DaemonMaxConnections:   g_daemonMaxConnections,
			DaemonMaxCalls:         g_daemonMaxCalls,
			DaemonSnapshotCacheTtl: g_daemonSnapshotCacheTtl,
		}
	} else {
		api = &core.RealSession{
			HostName:      g_hostName,
			ApiKey:        g_apiKey,
			IsDebug:       g_debug,
			AllowInsecure: g_allowInsecure,
		}
	}

	return api
}

type daemonSettings struct {
	maxConnections   int
	maxCalls         int
	snapshotCacheTtl string
}

// The daemon is shared by all hosts, so its settings live in an optional top-level "daemon"
// section of the config file. It's read even when --host and --api-key are given (as Incus does):
//
//	"daemon": {"max_connections": 2, "max_calls": 4, "snapshot_cache_ttl": "5s"}
//
// The settings are passed to the daemon when it gets launched; a daemon that is already
// running keeps its settings until it exits. Invalid values are reported and ignored.
func loadDaemonSettings(fileName string) daemonSettings {
	settings := daemonSettings{}
	if fileName == "" {
		fileName = getDefaultConfigPath()
	}
	data, err := os.ReadFile(fileName)
	if err != nil && fileName != getDefaultConfigPath() {
		fileName = getDefaultConfigPath()
		data, err = os.ReadFile(fileName)
	}
	if err != nil {
		return settings
	}
	var config map[string]interface{}
	if json.Unmarshal(data, &config) != nil {
		return settings
	}
	daemon, exists := config["daemon"]
	if !exists {
		return settings
	}
	daemonMap, ok := daemon.(map[string]interface{})
	if !ok {
		warnDaemonSetting(fileName, "daemon", "an object")
		return settings
	}
	readCount := func(key string) int {
		value, exists := daemonMap[key]
		if !exists {
			return 0
		}
		if n, ok := value.(float64); ok && n >= 1 && n == float64(int(n)) {
			return int(n)
		}
		warnDaemonSetting(fileName, "daemon."+key, "a whole number >= 1")
		return 0
	}
	settings.maxConnections = readCount("max_connections")
	settings.maxCalls = readCount("max_calls")
	if value, exists := daemonMap["snapshot_cache_ttl"]; exists {
		str, _ := value.(string)
		if _, err := time.ParseDuration(str); err == nil {
			settings.snapshotCacheTtl = str
		} else {
			warnDaemonSetting(fileName, "daemon.snapshot_cache_ttl", "a duration like \"5s\"")
		}
	}
	return settings
}

func warnDaemonSetting(fileName, key, expected string) {
	fmt.Fprintf(os.Stderr, "Warning: ignoring %s in \"%s\": expected %s\n", key, fileName, expected)
}

func applyDaemonSettings(settings daemonSettings) {
	g_daemonMaxConnections = settings.maxConnections
	g_daemonMaxCalls = settings.maxCalls
	g_daemonSnapshotCacheTtl = settings.snapshotCacheTtl
}

// This method is called assuming that we're missing either a hostname or api key.
// Additionally, we might not know the config path (in which case we use the default),
// or the name (in which case we just pick the first config in the list)
func findCredsFromConfig(fileName, name, existingHost, existingApiKey string) (string, string, map[string]interface{}, error) {
	var data []byte
	var err error

	if fileName == "" {
		fileName = getDefaultConfigPath()
		data, err = os.ReadFile(fileName)
	} else {
		data, err = os.ReadFile(fileName)
		if err != nil {
			fileName = getDefaultConfigPath()
			data, err = os.ReadFile(fileName)
		}
	}

	if err != nil {
		return "", "", nil, err
	}

	var obj interface{}
	if err = json.Unmarshal(data, &obj); err != nil {
		return "", "", nil, fmt.Errorf("\"%s\": %v", fileName, err)
	}

	jsonObj, ok := obj.(map[string]interface{})
	if !ok {
		return "", "", nil, fmt.Errorf("Config was not a JSON object \"%s\"", fileName)
	}

	hosts, err := getMapFromMapAny(jsonObj, "hosts", fileName)
	if err != nil {
		return "", "", nil, err
	}

	if name == "" {
		if existingHost != "" {
			existingHostCondensed := core.GetHostNameFromApiUrl(existingHost)
			for key, value := range hosts {
				if valueMap, ok := value.(map[string]interface{}); ok {
					url, _ := valueMap["url"].(string)
					if url != "" && core.GetHostNameFromApiUrl(url) == existingHostCondensed {
						name = key
						break
					}
				}
			}
		} else if existingApiKey != "" {
			for key, value := range hosts {
				if valueMap, ok := value.(map[string]interface{}); ok {
					apiKey, _ := valueMap["api_key"].(string)
					if apiKey == existingApiKey {
						name = key
						break
					}
				}
			}
		} else {
			for key, _ := range hosts {
				if name == "" || key < name {
					name = key
				}
			}
		}
		if name == "" {
			return "", "", nil, fmt.Errorf("Could not find any matching hosts in config \"%s\"", fileName)
		}
	}

	config, err := getMapFromMapAny(hosts, name, fileName)
	if err != nil {
		return "", "", nil, err
	}

	apiKey, err := getNonEmptyStringFromMapAny(config, "api_key", fileName)
	if err != nil {
		return "", "", nil, err
	}

	u, err := getNonEmptyStringFromMapAny(config, "url", fileName)
	if err != nil {
		return "", "", nil, err
	}

	return u, apiKey, config, nil
}

func getHomeDirWithFallback() (string, error) {
	// Try os.UserHomeDir() first
	if p, err := os.UserHomeDir(); err == nil {
		return p, nil
	}

	// Fallback strategies for container environments like Incus

	// Try $HOME environment variable directly
	if home := os.Getenv("HOME"); home != "" {
		return home, nil
	}

	// Try current working directory as last resort
	if cwd, err := os.Getwd(); err == nil {
		return cwd, nil
	}

	// If all else fails, use /tmp
	return "/tmp", nil
}

func getDefaultConfigPath() string {
	p, err := getHomeDirWithFallback()
	if err != nil {
		log.Fatal(err)
	}
	return path.Join(p, ".truenas_incus_ctl", "config.json")
}

func getMapFromMapAny(dict map[string]interface{}, key, fileName string) (map[string]interface{}, error) {
	var inner map[string]interface{}
	if innerObj, exists := dict[key]; exists {
		var ok bool
		inner, ok = innerObj.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("\"%s\" in config \"%s\" was not a JSON object", key, fileName)
		}
	} else {
		return nil, fmt.Errorf("Could not find \"%s\" in config \"%s\"", key, fileName)
	}
	return inner, nil
}

func getNonEmptyStringFromMapAny(dict map[string]interface{}, key, fileName string) (string, error) {
	var str string
	if strObj, exists := dict[key]; exists {
		var ok bool
		str, ok = strObj.(string)
		if !ok {
			return "", fmt.Errorf("\"%s\" in config \"%s\" was not a string", key, fileName)
		}
	} else {
		return "", fmt.Errorf("Could not find \"%s\" in config \"%s\"", key, fileName)
	}
	if str == "" {
		return "", fmt.Errorf("\"%s\" in config \"%s\" was left blank", key, fileName)
	}
	return str, nil
}

func DebugString(str string) {
	if g_debug {
		fmt.Println(str)
	}
}

func DebugJson(obj interface{}) {
	if g_debug {
		data, err := json.Marshal(obj)
		if err != nil {
			fmt.Printf("%v (%v)", obj, err)
		}
		fmt.Println(string(data))
	}
}
