// Package egressauth implements the worker side of the egress-authd
// delegation-grant relay.
//
// The egress authentication daemon ("egress-authd") runs as a per-node
// sidecar. The invoking client (e.g. the Bazel client) supplies a
// delegation grant -- a short, signed JWT -- as a gRPC metadata header.
// The scheduler forwards that header to the worker (see the scheduler's
// forward_request_headers allowlist and remoteworker.ForwardedRequestHeader),
// and before a build action runs the worker relays the grant to the
// daemon over a Unix domain socket. The daemon validates the grant,
// allocates a short-lived authenticated egress proxy scoped to it, and
// returns a set of environment variables (e.g. HTTPS_PROXY) -- and
// optionally a set of files (e.g. a credential-helper config) -- that
// the worker injects into the action's execution environment and input
// root.
//
// The grant itself is NEVER handed to the action: it is relayed
// out-of-band to the daemon only. The action only ever sees the returned
// proxy environment variables (which point at a loopback port held open
// by the daemon for the duration of the action) and any returned files.
// No grant or credential is read by or written to the action's command
// or the action digest.
package egressauth

import (
	"context"
)

// Action is the context that the worker relays to the egress
// authentication daemon for a single build action.
type Action struct {
	// Grant is the delegation grant (a signed JWT) supplied by the
	// invoking client and forwarded to the worker as a gRPC metadata
	// header. It is relayed to the daemon verbatim and is never exposed
	// to the action.
	Grant string
}

// File is a file that the egress authentication daemon asks the worker to
// materialize in the action's input root before execution (e.g. a
// credential-helper configuration file).
type File struct {
	// Path is the location of the file, relative to the action's input
	// root.
	Path string

	// Contents is the verbatim file content to write.
	Contents []byte
}

// Registration is the result of relaying an action's grant to the egress
// authentication daemon.
type Registration struct {
	// ActionID is the daemon-assigned handle for the registration. It
	// is passed back to the daemon to release the proxy once the action
	// completes.
	ActionID string

	// Environment variables to inject into the action's execution
	// environment (e.g. {"HTTPS_PROXY": "http://127.0.0.1:43521"}).
	Environment map[string]string

	// Files to materialize in the action's input root before execution.
	// May be empty.
	Files []File
}

// Relay registers and releases per-action egress authentication against
// the egress-authd sidecar.
type Relay interface {
	// Register relays the delegation grant to the daemon and returns the
	// environment variables (and optional files) to inject into the
	// action, plus a handle used to release the registration afterwards.
	Register(ctx context.Context, action Action) (*Registration, error)

	// Release tears down a registration previously returned by Register.
	// It is best-effort: callers should log but not fail on errors.
	Release(ctx context.Context, actionID string) error
}
