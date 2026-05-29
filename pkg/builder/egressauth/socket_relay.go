package egressauth

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The control API is served over HTTP on a Unix domain socket. The host
// part of the URL is ignored by the daemon; we use a fixed placeholder.
const controlHost = "egress-authd"

// controlTimeout bounds a single control-plane request to the egress
// authentication daemon, so a hung daemon cannot stall an action
// indefinitely. The control plane is a local Unix domain socket, so
// requests are expected to complete quickly.
const controlTimeout = 30 * time.Second

// registerRequest is the body of "POST /actions".
type registerRequest struct {
	Grant     string `json:"grant"`
	ExpiresAt string `json:"expires_at"`
}

// registerResponseFile is a single entry of the optional "files" array in
// a successful "POST /actions" response. Contents is a PLAINTEXT string:
// the daemon emits the file body verbatim (e.g. a cargo/docker/git config),
// not base64. Declaring it []byte would make encoding/json apply
// base64-StdEncoding decoding and corrupt (or reject) the plaintext.
type registerResponseFile struct {
	Path     string `json:"path"`
	Contents string `json:"contents"`
}

// registerResponse is the body of a successful "POST /actions".
type registerResponse struct {
	ActionID    string                 `json:"action_id"`
	Environment map[string]string      `json:"env"`
	Files       []registerResponseFile `json:"files"`
}

// socketRelay talks the egress-authd control protocol over a Unix domain
// socket.
type socketRelay struct {
	httpClient *http.Client
	clock      clock.Clock
	// ttl bounds the lifetime of a registration if the worker fails to
	// release it (e.g. crashes). The daemon is expected to reclaim the
	// proxy after expires_at.
	ttl time.Duration
}

// NewSocketRelay creates a Relay that communicates with the egress
// authentication daemon over the Unix domain socket at controlSocket.
func NewSocketRelay(controlSocket string, ttl time.Duration, clock clock.Clock) Relay {
	dialer := &net.Dialer{}
	return &socketRelay{
		httpClient: &http.Client{
			Timeout: controlTimeout,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return dialer.DialContext(ctx, "unix", controlSocket)
				},
			},
		},
		clock: clock,
		ttl:   ttl,
	}
}

func (r *socketRelay) Register(ctx context.Context, action Action) (*Registration, error) {
	body, err := json.Marshal(registerRequest{
		Grant:     action.Grant,
		ExpiresAt: r.clock.Now().Add(r.ttl).UTC().Format(time.RFC3339),
	})
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to marshal egress authentication request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+controlHost+"/actions", bytes.NewReader(body))
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to create egress authentication request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unavailable, "Failed to contact egress authentication daemon")
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unavailable, "Failed to read egress authentication response")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, status.Errorf(codes.Unavailable, "Egress authentication daemon returned status %d: %s", resp.StatusCode, bytes.TrimSpace(respBody))
	}
	var parsed registerResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to parse egress authentication response")
	}
	if parsed.ActionID == "" {
		return nil, status.Error(codes.Internal, "Egress authentication daemon returned an empty action ID")
	}
	registration := &Registration{
		ActionID:    parsed.ActionID,
		Environment: parsed.Environment,
	}
	for _, f := range parsed.Files {
		registration.Files = append(registration.Files, File{
			Path: f.Path,
			// The daemon sends plaintext contents; File.Contents is []byte
			// (it is written verbatim to the input root), so convert.
			Contents: []byte(f.Contents),
		})
	}
	return registration, nil
}

func (r *socketRelay) Release(ctx context.Context, actionID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://"+controlHost+"/actions/"+actionID, nil)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create egress authentication release request")
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to contact egress authentication daemon")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return status.Errorf(codes.Unavailable, "Egress authentication daemon returned status %d on release", resp.StatusCode)
	}
	return nil
}
