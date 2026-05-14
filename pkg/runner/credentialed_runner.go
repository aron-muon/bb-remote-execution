package runner

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type netrcEntry struct {
	machine  string
	login    string
	password string
}

type credentialedRunner struct {
	base          runner_pb.RunnerServer
	broker        *brokerClient
	destinations  []*bb_runner.CredentialDestination
	delegationVar string
}

// NewCredentialedRunner decorates a RunnerServer with per-action
// credential injection. On each Run, if the configured delegation
// environment variable is present, the runner exchanges it for real
// upstream credentials via the bb-credential-broker and writes
// credential files into the action's input root. The delegation
// variable is removed from the environment before the action spawns.
//
// When the delegation variable is absent, the request is forwarded
// to the base runner unmodified (passthrough mode).
func NewCredentialedRunner(base runner_pb.RunnerServer, cfg *bb_runner.CredentialInjectionConfiguration) runner_pb.RunnerServer {
	delegationVar := cfg.DelegationEnvVar
	if delegationVar == "" {
		delegationVar = "BB_DELEGATION_JWT"
	}
	return &credentialedRunner{
		base:          base,
		broker:        newBrokerClient(cfg.BrokerUrl),
		destinations:  cfg.Destinations,
		delegationVar: delegationVar,
	}
}

func (r *credentialedRunner) CheckReadiness(ctx context.Context, request *runner_pb.CheckReadinessRequest) (*emptypb.Empty, error) {
	return r.base.CheckReadiness(ctx, request)
}

func (r *credentialedRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	jwt, hasJWT := request.EnvironmentVariables[r.delegationVar]
	if !hasJWT {
		return r.base.Run(ctx, request)
	}

	delete(request.EnvironmentVariables, r.delegationVar)

	if err := r.injectCredentials(ctx, jwt, request.InputRootDirectory); err != nil {
		return nil, status.Errorf(codes.Internal, "credential injection failed: %v", err)
	}

	return r.base.Run(ctx, request)
}

func (r *credentialedRunner) injectCredentials(ctx context.Context, jwt, inputRoot string) error {
	netrcByPath := map[string][]netrcEntry{}

	for _, dest := range r.destinations {
		token, err := r.broker.fetchToken(ctx, jwt, dest.Name)
		if err != nil {
			return fmt.Errorf("destination %q: %w", dest.Name, err)
		}
		log.Printf("credentialed_runner: minted token for destination %q", dest.Name)

		for _, cf := range dest.CredentialFiles {
			switch cf.Type {
			case "netrc":
				netrcByPath[cf.Path] = append(netrcByPath[cf.Path], netrcEntry{
					machine:  cf.Machine,
					login:    cf.Login,
					password: token,
				})
			default:
				return fmt.Errorf("destination %q: unsupported credential file type %q", dest.Name, cf.Type)
			}
		}
	}

	for relPath, entries := range netrcByPath {
		absPath := filepath.Join(inputRoot, relPath)
		if err := writeNetrc(absPath, entries); err != nil {
			return fmt.Errorf("write %s: %w", relPath, err)
		}
	}

	return nil
}

func writeNetrc(path string, entries []netrcEntry) error {
	var b strings.Builder
	for i, e := range entries {
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "machine %s\nlogin %s\npassword %s\n", e.machine, e.login, e.password)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
