package egressauth_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/builder/egressauth"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/stretchr/testify/require"
)

// startControlServer starts an in-process egress-authd control server on a
// Unix domain socket, returning the socket path.
func startControlServer(t *testing.T, handler http.Handler) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("egress-authd uses a Unix domain socket control plane; the worker relay runs on Linux")
	}
	dir := t.TempDir()
	socket := filepath.Join(dir, "control.sock")
	l, err := net.Listen("unix", socket)
	require.NoError(t, err)
	srv := &http.Server{Handler: handler}
	go srv.Serve(l)
	t.Cleanup(func() {
		srv.Close()
		os.Remove(socket)
	})
	return socket
}

func TestSocketRelayRegisterRelease(t *testing.T) {
	var gotRegister registerBody
	socket := startControlServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/actions":
			body, _ := io.ReadAll(r.Body)
			require.NoError(t, json.Unmarshal(body, &gotRegister))
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"action_id":  "act-123",
				"proxy_port": 43521,
				"env":        map[string]string{"HTTPS_PROXY": "http://127.0.0.1:43521"},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/actions/act-123":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	relay := egressauth.NewSocketRelay(socket, time.Hour, clock.SystemClock)
	reg, err := relay.Register(context.Background(), egressauth.Action{
		Grant: "signed.jwt.grant",
	})
	require.NoError(t, err)
	require.Equal(t, "act-123", reg.ActionID)
	require.Equal(t, map[string]string{"HTTPS_PROXY": "http://127.0.0.1:43521"}, reg.Environment)
	require.Empty(t, reg.Files)

	// The relayed grant matches what was registered, and an expiry is set.
	require.Equal(t, "signed.jwt.grant", gotRegister.Grant)
	require.NotEmpty(t, gotRegister.ExpiresAt)
	_, err = time.Parse(time.RFC3339, gotRegister.ExpiresAt)
	require.NoError(t, err)

	require.NoError(t, relay.Release(context.Background(), "act-123"))
}

func TestSocketRelayRegisterWithFiles(t *testing.T) {
	socket := startControlServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/actions", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"action_id":  "act-files",
			"proxy_port": 1234,
			"env":        map[string]string{"HTTPS_PROXY": "http://127.0.0.1:1234"},
			// The daemon emits PLAINTEXT file contents (verbatim config
			// bodies), not base64. The relay reads the JSON string and
			// converts it to the []byte File.Contents written to the input
			// root.
			"files": []map[string]any{
				{"path": ".netrc", "contents": "machine example.com login x password y"},
			},
		})
	}))

	relay := egressauth.NewSocketRelay(socket, time.Hour, clock.SystemClock)
	reg, err := relay.Register(context.Background(), egressauth.Action{Grant: "g"})
	require.NoError(t, err)
	require.Equal(t, "act-files", reg.ActionID)
	require.Len(t, reg.Files, 1)
	require.Equal(t, ".netrc", reg.Files[0].Path)
	require.Equal(t, []byte("machine example.com login x password y"), reg.Files[0].Contents)
}

// TestSocketRelayRegisterPreservesPlaintextConfig locks in the plaintext
// contents contract with a realistic config body (TOML with brackets,
// '=', and newlines). Under the previous []byte typing encoding/json
// would attempt base64-StdEncoding decoding and either corrupt this or
// fail outright; as a plaintext string it must round-trip verbatim.
func TestSocketRelayRegisterPreservesPlaintextConfig(t *testing.T) {
	const cargoConfig = "[source.crates-io]\nreplace-with = \"egress-authd\"\n\n[source.egress-authd]\nregistry = \"sparse+http://127.0.0.1:1234/registry-cargo/\"\n"
	socket := startControlServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"action_id":  "act-toml",
			"proxy_port": 1234,
			"env":        map[string]string{},
			"files": []map[string]any{
				{"path": ".cargo/config.toml", "contents": cargoConfig},
			},
		})
	}))

	relay := egressauth.NewSocketRelay(socket, time.Hour, clock.SystemClock)
	reg, err := relay.Register(context.Background(), egressauth.Action{Grant: "g"})
	require.NoError(t, err)
	require.Len(t, reg.Files, 1)
	require.Equal(t, ".cargo/config.toml", reg.Files[0].Path)
	require.Equal(t, []byte(cargoConfig), reg.Files[0].Contents)
}

func TestSocketRelayRegisterServerError(t *testing.T) {
	socket := startControlServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	relay := egressauth.NewSocketRelay(socket, time.Hour, clock.SystemClock)
	_, err := relay.Register(context.Background(), egressauth.Action{Grant: "g"})
	require.Error(t, err)
}

type registerBody struct {
	Grant     string `json:"grant"`
	ExpiresAt string `json:"expires_at"`
}
