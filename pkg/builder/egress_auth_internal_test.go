package builder

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/builder/egressauth"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// fakeRelay is a hand-written egressauth.Relay. The generated gomock
// cannot be used here: this is an internal (package builder) test, and the
// mock package depends on package builder, which would be an import cycle.
type fakeRelay struct {
	registration *egressauth.Registration
	registerErr  error

	gotAction  egressauth.Action
	registered bool
	released   string
}

func (r *fakeRelay) Register(_ context.Context, action egressauth.Action) (*egressauth.Registration, error) {
	r.registered = true
	r.gotAction = action
	if r.registerErr != nil {
		return nil, r.registerErr
	}
	return r.registration, nil
}

func (r *fakeRelay) Release(_ context.Context, actionID string) error {
	r.released = actionID
	return nil
}

func mustGrantHeader(t *testing.T, value string) *anypb.Any {
	t.Helper()
	a, err := anypb.New(&remoteworker.ForwardedRequestHeader{
		Name:  egressauth.DefaultGrantHeaderName,
		Value: value,
	})
	require.NoError(t, err)
	return a
}

// TestApplyEgressAuth covers the worker-side grant-relay wiring: fail-open
// when egress authentication is disabled or no grant is present,
// register+inject+release when a grant is present, and error propagation
// when the daemon rejects the registration.
func TestApplyEgressAuth(t *testing.T) {
	ctx := context.Background()
	aux := []*anypb.Any{mustGrantHeader(t, "signed.jwt.grant")}

	t.Run("DisabledFailOpen", func(t *testing.T) {
		be := &localBuildExecutor{}
		environmentVariables := map[string]string{}
		cleanup, err := be.applyEgressAuth(ctx, aux, environmentVariables, nil)
		require.NoError(t, err)
		require.NotNil(t, cleanup)
		cleanup() // no registration: must be a safe no-op
		require.Empty(t, environmentVariables)
	})

	t.Run("NoGrantHeaderFailOpen", func(t *testing.T) {
		relay := &fakeRelay{}
		be := &localBuildExecutor{
			egressAuthRelay:           relay,
			egressAuthGrantHeaderName: egressauth.DefaultGrantHeaderName,
		}
		environmentVariables := map[string]string{}
		cleanup, err := be.applyEgressAuth(ctx, nil, environmentVariables, nil)
		require.NoError(t, err)
		cleanup()
		require.False(t, relay.registered, "Register must not be called without a grant header")
		require.Empty(t, environmentVariables)
	})

	t.Run("RegisterInjectsEnvAndReleases", func(t *testing.T) {
		relay := &fakeRelay{registration: &egressauth.Registration{
			ActionID:    "act-1",
			Environment: map[string]string{"HTTPS_PROXY": "http://127.0.0.1:5000"},
		}}
		be := &localBuildExecutor{
			egressAuthRelay:           relay,
			egressAuthGrantHeaderName: egressauth.DefaultGrantHeaderName,
		}
		environmentVariables := map[string]string{"PATH": "/usr/bin"}
		cleanup, err := be.applyEgressAuth(ctx, aux, environmentVariables, nil)
		require.NoError(t, err)
		require.Equal(t, "signed.jwt.grant", relay.gotAction.Grant)
		// Proxy env injected; pre-existing (digest-committed) env preserved.
		require.Equal(t, "http://127.0.0.1:5000", environmentVariables["HTTPS_PROXY"])
		require.Equal(t, "/usr/bin", environmentVariables["PATH"])
		cleanup()
		require.Equal(t, "act-1", relay.released)
	})

	t.Run("RegisterErrorFailsClosed", func(t *testing.T) {
		relay := &fakeRelay{registerErr: status.Error(codes.Unavailable, "daemon down")}
		be := &localBuildExecutor{
			egressAuthRelay:           relay,
			egressAuthGrantHeaderName: egressauth.DefaultGrantHeaderName,
		}
		environmentVariables := map[string]string{}
		cleanup, err := be.applyEgressAuth(ctx, aux, environmentVariables, nil)
		require.Error(t, err)
		cleanup()
		require.Empty(t, relay.released, "Release must not be called when registration failed")
		require.Empty(t, environmentVariables)
	})
}

// fakeWritableDirectory is a minimal filesystem.Directory that records
// files written through OpenWrite. Only the methods writeEgressAuthFile
// exercises are implemented; the embedded nil interface panics on any
// other call, keeping the fake honest.
type fakeWritableDirectory struct {
	filesystem.Directory
	written map[string][]byte
}

func (d *fakeWritableDirectory) OpenWrite(name path.Component, _ filesystem.CreationMode) (filesystem.FileWriter, error) {
	return &fakeFileWriter{dir: d, name: name.String()}, nil
}

type fakeFileWriter struct {
	filesystem.FileWriter
	dir  *fakeWritableDirectory
	name string
	buf  []byte
}

func (w *fakeFileWriter) WriteAt(p []byte, _ int64) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *fakeFileWriter) Close() error {
	w.dir.written[w.name] = w.buf
	return nil
}

// TestWriteEgressAuthFile verifies daemon-supplied file paths cannot
// escape the input root, and that a root-level file is written verbatim.
func TestWriteEgressAuthFile(t *testing.T) {
	t.Run("RejectsUnsafePaths", func(t *testing.T) {
		// Each path is rejected by validation before the directory is
		// touched, so a nil root is never dereferenced.
		for _, unsafe := range []string{"../escape", "/abs/path", "a/../b", "a/./b", ".", "..", "sub/", ""} {
			err := writeEgressAuthFile(nil, egressauth.File{Path: unsafe, Contents: []byte("x")})
			require.Errorf(t, err, "path %q must be rejected", unsafe)
		}
	})

	t.Run("WritesRootLevelFile", func(t *testing.T) {
		root := &fakeWritableDirectory{written: map[string][]byte{}}
		contents := []byte("machine example.com login x password y")
		require.NoError(t, writeEgressAuthFile(root, egressauth.File{Path: ".netrc", Contents: contents}))
		require.Equal(t, contents, root.written[".netrc"])
	})
}
