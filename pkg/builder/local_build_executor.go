package builder

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder/egressauth"
	re_clock "github.com/buildbarn/bb-remote-execution/pkg/clock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Filenames of objects to be created inside the build directory.
var (
	stdoutComponent              = path.MustNewComponent("stdout")
	stderrComponent              = path.MustNewComponent("stderr")
	deviceDirectoryComponent     = path.MustNewComponent("dev")
	inputRootDirectoryComponent  = path.MustNewComponent("root")
	serverLogsDirectoryComponent = path.MustNewComponent("server_logs")
	temporaryDirectoryComponent  = path.MustNewComponent("tmp")
	checkReadinessComponent      = path.MustNewComponent("check_readiness")
)

// capturingErrorLogger is an error logger that stores up to a single
// error. When the error is stored, a context cancelation function is
// invoked. This is used by localBuildExecutor to kill a build action in
// case an I/O error occurs on the FUSE file system.
type capturingErrorLogger struct {
	lock   sync.Mutex
	cancel context.CancelFunc
	error  error
}

func (el *capturingErrorLogger) Log(err error) {
	el.lock.Lock()
	defer el.lock.Unlock()

	if el.cancel != nil {
		el.error = err
		el.cancel()
		el.cancel = nil
	}
}

func (el *capturingErrorLogger) GetError() error {
	el.lock.Lock()
	defer el.lock.Unlock()

	return el.error
}

type localBuildExecutor struct {
	contentAddressableStorage      blobstore.BlobAccess
	buildDirectoryCreator          BuildDirectoryCreator
	runner                         runner_pb.RunnerClient
	clock                          clock.Clock
	maximumWritableFileUploadDelay time.Duration
	inputRootCharacterDevices      map[path.Component]filesystem.DeviceNumber
	maximumMessageSizeBytes        int
	environmentVariables           map[string]string
	forceUploadTreesAndDirectories bool
	egressAuthRelay                egressauth.Relay
	egressAuthGrantHeaderName      string
}

// NewLocalBuildExecutor returns a BuildExecutor that executes build
// steps on the local system.
//
// If egressAuthRelay is non-nil, the worker looks for a delegation grant
// forwarded by the scheduler as a remoteworker.ForwardedRequestHeader
// named egressAuthGrantHeaderName. When such a grant is present, it is
// relayed to the egress authentication daemon before the action runs, and
// the proxy environment variables (and optional files) the daemon returns
// are injected into the action at execution time. The grant itself is
// never injected into the action's environment, command, or input root,
// and never becomes part of the action digest.
//
// When no grant header is present, the action runs normally without
// sidecar routing (fail-open): actions that do not require authenticated
// egress are unaffected.
func NewLocalBuildExecutor(contentAddressableStorage blobstore.BlobAccess, buildDirectoryCreator BuildDirectoryCreator, runner runner_pb.RunnerClient, clock clock.Clock, maximumWritableFileUploadDelay time.Duration, inputRootCharacterDevices map[path.Component]filesystem.DeviceNumber, maximumMessageSizeBytes int, environmentVariables map[string]string, forceUploadTreesAndDirectories bool, egressAuthRelay egressauth.Relay, egressAuthGrantHeaderName string) BuildExecutor {
	return &localBuildExecutor{
		contentAddressableStorage:      contentAddressableStorage,
		buildDirectoryCreator:          buildDirectoryCreator,
		runner:                         runner,
		clock:                          clock,
		maximumWritableFileUploadDelay: maximumWritableFileUploadDelay,
		inputRootCharacterDevices:      inputRootCharacterDevices,
		maximumMessageSizeBytes:        maximumMessageSizeBytes,
		environmentVariables:           environmentVariables,
		forceUploadTreesAndDirectories: forceUploadTreesAndDirectories,
		egressAuthRelay:                egressAuthRelay,
		egressAuthGrantHeaderName:      egressAuthGrantHeaderName,
	}
}

func (be *localBuildExecutor) createCharacterDevices(inputRootDirectory BuildDirectory) error {
	if err := inputRootDirectory.Mkdir(deviceDirectoryComponent, 0o777); err != nil && !os.IsExist(err) {
		return util.StatusWrap(err, "Unable to create /dev directory in input root")
	}
	deviceDirectory, err := inputRootDirectory.EnterBuildDirectory(deviceDirectoryComponent)
	if err != nil {
		return util.StatusWrap(err, "Unable to enter /dev directory in input root")
	}
	defer deviceDirectory.Close()
	for name, number := range be.inputRootCharacterDevices {
		if err := deviceDirectory.Mknod(name, os.ModeDevice|os.ModeCharDevice|0o666, number); err != nil {
			return util.StatusWrapf(err, "Failed to create character device %#v", name.String())
		}
	}
	return nil
}

func (be *localBuildExecutor) CheckReadiness(ctx context.Context) error {
	buildDirectory, buildDirectoryPath, err := be.buildDirectoryCreator.GetBuildDirectory(ctx, nil)
	if err != nil {
		return util.StatusWrap(err, "Failed to get build directory")
	}
	defer buildDirectory.Close()

	// Create a useless directory inside the build directory. The
	// runner will validate that it exists.
	if err := buildDirectory.Mkdir(checkReadinessComponent, 0o777); err != nil {
		return util.StatusWrap(err, "Failed to create readiness checking directory")
	}
	_, err = be.runner.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{
		Path: buildDirectoryPath.Append(checkReadinessComponent).GetUNIXString(),
	})
	return err
}

func (be *localBuildExecutor) Execute(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	// Timeout handling.
	response := NewDefaultExecuteResponse(request)
	action := request.Action
	if action == nil {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain an action"))
		return response
	}
	if err := action.Timeout.CheckValid(); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid execution timeout"))
		return response
	}
	executionTimeout := action.Timeout.AsDuration()

	// Obtain build directory.
	actionDigest, err := digestFunction.NewDigestFromProto(request.ActionDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
		return response
	}
	var actionDigestIfNotRunInParallel *digest.Digest
	if !action.DoNotCache {
		actionDigestIfNotRunInParallel = &actionDigest
	}
	buildDirectory, buildDirectoryPath, err := be.buildDirectoryCreator.GetBuildDirectory(ctx, actionDigestIfNotRunInParallel)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to acquire build environment"))
		return response
	}
	defer func() {
		err := buildDirectory.Close()
		if err != nil {
			attachErrorToExecuteResponse(
				response,
				util.StatusWrap(err, "Failed to close build directory"))
		}
	}()

	// Install hooks on build directory to capture file creation and
	// I/O error events.
	ctxWithIOError, cancelIOError := context.WithCancel(ctx)
	defer cancelIOError()
	ioErrorCapturer := capturingErrorLogger{cancel: cancelIOError}
	buildDirectory.InstallHooks(filePool, &ioErrorCapturer)

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
			FetchingInputs: &emptypb.Empty{},
		},
	}

	// Create input root directory inside of build directory.
	if err := buildDirectory.Mkdir(inputRootDirectoryComponent, 0o777); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to create input root directory"))
		return response
	}
	inputRootDirectory, err := buildDirectory.EnterBuildDirectory(inputRootDirectoryComponent)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to enter input root directory"))
		return response
	}
	defer inputRootDirectory.Close()

	inputRootDigest, err := digestFunction.NewDigestFromProto(action.InputRootDigest)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to extract digest for input root"))
		return response
	}
	if err := inputRootDirectory.MergeDirectoryContents(ctx, &ioErrorCapturer, inputRootDigest, monitor); err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}

	if len(be.inputRootCharacterDevices) > 0 {
		if err := be.createCharacterDevices(inputRootDirectory); err != nil {
			attachErrorToExecuteResponse(response, err)
			return response
		}
	}

	// Create parent directories of output files and directories.
	// These are not declared in the input root explicitly.
	commandDigest, err := digestFunction.NewDigestFromProto(action.CommandDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for command"))
		return response
	}
	commandMessage, err := be.contentAddressableStorage.Get(ctx, commandDigest).ToProto(&remoteexecution.Command{}, be.maximumMessageSizeBytes)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to obtain command"))
		return response
	}
	command := commandMessage.(*remoteexecution.Command)
	outputHierarchy, err := NewOutputHierarchy(command)
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
	if err := outputHierarchy.CreateParentDirectories(inputRootDirectory); err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}

	// Create a directory inside the build directory that build
	// actions may use to store temporary files. This ensures that
	// temporary files are automatically removed when the build
	// action completes. When using FUSE, it also causes quotas to
	// be applied to them.
	if err := buildDirectory.Mkdir(temporaryDirectoryComponent, 0o777); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to create temporary directory inside build directory"))
		return response
	}

	if err := buildDirectory.Mkdir(serverLogsDirectoryComponent, 0o777); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to create server logs directory inside build directory"))
		return response
	}

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_Running{
			Running: &emptypb.Empty{},
		},
	}

	environmentVariables := map[string]string{}
	for name, value := range be.environmentVariables {
		environmentVariables[name] = value
	}
	for _, environmentVariable := range command.EnvironmentVariables {
		environmentVariables[environmentVariable.Name] = environmentVariable.Value
	}

	// Relay a client-supplied delegation grant to the egress-authd sidecar
	// (when enabled and present) and inject the proxy environment variables
	// (and any files) it returns. See applyEgressAuth: the grant is never
	// written to the action's environment, command, input root, or digest,
	// and the action runs normally when no grant is present (fail-open).
	egressAuthCleanup, err := be.applyEgressAuth(ctx, request.AuxiliaryMetadata, environmentVariables, inputRootDirectory)
	defer egressAuthCleanup()
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}

	// Invoke the command.
	ctxWithTimeout, cancelTimeout := be.clock.NewContextWithTimeout(ctxWithIOError, executionTimeout)
	runResponse, runErr := be.runner.Run(ctxWithTimeout, &runner_pb.RunRequest{
		Arguments:            command.Arguments,
		EnvironmentVariables: environmentVariables,
		WorkingDirectory:     command.WorkingDirectory,
		StdoutPath:           buildDirectoryPath.Append(stdoutComponent).GetUNIXString(),
		StderrPath:           buildDirectoryPath.Append(stderrComponent).GetUNIXString(),
		InputRootDirectory:   buildDirectoryPath.Append(inputRootDirectoryComponent).GetUNIXString(),
		TemporaryDirectory:   buildDirectoryPath.Append(temporaryDirectoryComponent).GetUNIXString(),
		ServerLogsDirectory:  buildDirectoryPath.Append(serverLogsDirectoryComponent).GetUNIXString(),
	})
	cancelTimeout()
	<-ctxWithTimeout.Done()

	// If an I/O error occurred during execution, attach any errors
	// related to it to the response first. These errors should be
	// preferred over the cancelation errors that are a result of it.
	if err := ioErrorCapturer.GetError(); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "I/O error while running command"))
	}

	// Attach the exit code or execution error.
	if runErr == nil {
		response.Result.ExitCode = int32(runResponse.ExitCode)
		response.Result.ExecutionMetadata.AuxiliaryMetadata = append(response.Result.ExecutionMetadata.AuxiliaryMetadata, runResponse.ResourceUsage...)
	} else {
		attachErrorToExecuteResponse(response, util.StatusWrap(runErr, "Failed to run command"))
	}

	// For FUSE-based workers: Attach the amount of time the action
	// ran, minus the time it was delayed reading data from storage.
	if unsuspendedDuration, ok := ctxWithTimeout.Value(re_clock.UnsuspendedDurationKey{}).(time.Duration); ok {
		response.Result.ExecutionMetadata.VirtualExecutionDuration = durationpb.New(unsuspendedDuration)
	}

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_UploadingOutputs{
			UploadingOutputs: &emptypb.Empty{},
		},
	}

	writableFileUploadDelayCtx, writableFileUploadDelayCancel := be.clock.NewContextWithTimeout(ctx, be.maximumWritableFileUploadDelay)
	defer writableFileUploadDelayCancel()
	writableFileUploadDelayChan := writableFileUploadDelayCtx.Done()

	// Upload command output. In the common case, the stdout and
	// stderr files are empty. If that's the case, don't bother
	// setting the digest to keep the ActionResult small.
	if stdoutDigest, err := buildDirectory.UploadFile(ctx, stdoutComponent, digestFunction, writableFileUploadDelayChan); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stdout"))
	} else if stdoutDigest.GetSizeBytes() > 0 {
		response.Result.StdoutDigest = stdoutDigest.GetProto()
	}
	if stderrDigest, err := buildDirectory.UploadFile(ctx, stderrComponent, digestFunction, writableFileUploadDelayChan); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stderr"))
	} else if stderrDigest.GetSizeBytes() > 0 {
		response.Result.StderrDigest = stderrDigest.GetProto()
	}
	if err := outputHierarchy.UploadOutputs(ctx, inputRootDirectory, be.contentAddressableStorage, digestFunction, writableFileUploadDelayChan, response.Result, be.forceUploadTreesAndDirectories); err != nil {
		attachErrorToExecuteResponse(response, err)
	}

	// Recursively traverse the server logs directory and attach any
	// file stored within to the ExecuteResponse.
	serverLogsDirectoryUploader := serverLogsDirectoryUploader{
		context:                 ctx,
		executeResponse:         response,
		digestFunction:          digestFunction,
		writableFileUploadDelay: writableFileUploadDelayChan,
	}
	serverLogsDirectoryUploader.uploadDirectory(buildDirectory, serverLogsDirectoryComponent, nil)

	return response
}

// egressAuthReleaseTimeout bounds the best-effort release of an egress
// authentication registration. Release runs on a fresh context rather
// than the action's (which may already be cancelled when the action
// finishes or times out), so the sidecar is told to reclaim the proxy
// promptly instead of waiting for the registration's TTL to lapse.
const egressAuthReleaseTimeout = 10 * time.Second

// applyEgressAuth relays a client-supplied delegation grant to the
// egress-authd sidecar -- when egress authentication is enabled and a
// grant header is present in auxiliaryMetadata -- and injects the proxy
// environment variables the daemon returns into environmentVariables, plus
// any returned files into inputRootDirectory. It returns a cleanup
// function that releases the registration; the function is always non-nil
// and safe to call even when nothing was registered.
//
// The grant is relayed out-of-band only: it is never written to
// environmentVariables, the command, the input root, or the action
// digest. When egress authentication is disabled or no grant header is
// present, the action runs normally (fail-open): cleanup is a no-op and no
// error is returned.
func (be *localBuildExecutor) applyEgressAuth(ctx context.Context, auxiliaryMetadata []*anypb.Any, environmentVariables map[string]string, inputRootDirectory BuildDirectory) (func(), error) {
	noCleanup := func() {}
	if be.egressAuthRelay == nil {
		return noCleanup, nil
	}
	grant, ok := egressauth.ExtractGrant(auxiliaryMetadata, be.egressAuthGrantHeaderName)
	if !ok {
		return noCleanup, nil
	}
	registration, err := be.egressAuthRelay.Register(ctx, egressauth.Action{Grant: grant})
	if err != nil {
		return noCleanup, util.StatusWrap(err, "Failed to register action with the egress authentication daemon")
	}
	cleanup := func() {
		// Best-effort: a release failure must not affect the action's
		// result (the daemon reclaims the proxy after expires_at anyway).
		// A fresh context is used so release still reaches the daemon when
		// the action's context has already been cancelled.
		releaseCtx, cancel := context.WithTimeout(context.Background(), egressAuthReleaseTimeout)
		defer cancel()
		if err := be.egressAuthRelay.Release(releaseCtx, registration.ActionID); err != nil {
			util.DefaultErrorLogger.Log(util.StatusWrap(err, "Failed to release egress authentication registration"))
		}
	}
	// Inject the proxy environment variables into the execution-time map
	// (outside command.EnvironmentVariables, so outside the action digest).
	for name, value := range registration.Environment {
		environmentVariables[name] = value
	}
	// Materialize any files the daemon asked for (e.g. a credential-helper
	// config) into the action's input root before the action runs. These
	// files are required for authenticated egress to function, so a write
	// failure fails the action (the registration is still released via the
	// returned cleanup).
	if err := writeEgressAuthFiles(inputRootDirectory, registration.Files); err != nil {
		return cleanup, util.StatusWrap(err, "Failed to write egress authentication files into input root")
	}
	return cleanup, nil
}

// writeEgressAuthFiles materializes the files returned by the egress
// authentication daemon into the action's input root, prior to execution.
//
// Files are written through the filesystem.Directory interface, so this
// is supported for POSIX-backed (naive) build directories. The grant is
// never among these files: the daemon only ever returns non-secret helper
// material (e.g. a credential-helper configuration that points the
// action's tooling at the loopback proxy).
func writeEgressAuthFiles(inputRootDirectory BuildDirectory, files []egressauth.File) error {
	if len(files) == 0 {
		return nil
	}
	writableRoot, ok := any(inputRootDirectory).(filesystem.Directory)
	if !ok {
		return status.Error(codes.Unimplemented, "The build directory does not support writing egress authentication files")
	}
	for _, file := range files {
		if err := writeEgressAuthFile(writableRoot, file); err != nil {
			return util.StatusWrapf(err, "Failed to write %#v", file.Path)
		}
	}
	return nil
}

// writeEgressAuthFile writes a single file (creating any parent
// directories) at a path relative to the input root. The path is
// interpreted as a slash-separated relative path; absolute paths and "."
// or ".." components are rejected so a file can never escape the input
// root.
func writeEgressAuthFile(root filesystem.Directory, file egressauth.File) error {
	parts := strings.Split(file.Path, "/")
	dirComponents := make([]path.Component, 0, len(parts))
	for _, part := range parts[:len(parts)-1] {
		if part == "" || part == "." || part == ".." {
			return status.Errorf(codes.InvalidArgument, "Invalid path component %#v", part)
		}
		component, ok := path.NewComponent(part)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Invalid path component %#v", part)
		}
		dirComponents = append(dirComponents, component)
	}
	fileNameStr := parts[len(parts)-1]
	if fileNameStr == "" || fileNameStr == "." || fileNameStr == ".." {
		return status.Errorf(codes.InvalidArgument, "Invalid file name %#v", fileNameStr)
	}
	fileName, ok := path.NewComponent(fileNameStr)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "Invalid file name %#v", fileNameStr)
	}

	// Descend into (creating as needed) each intermediate directory,
	// then write the final component as a regular file.
	dir := filesystem.NopDirectoryCloser(root)
	defer func() { dir.Close() }()
	for _, component := range dirComponents {
		if err := dir.Mkdir(component, 0o777); err != nil && !os.IsExist(err) {
			return util.StatusWrapf(err, "Failed to create directory %#v", component.String())
		}
		child, err := dir.EnterDirectory(component)
		if err != nil {
			return util.StatusWrapf(err, "Failed to enter directory %#v", component.String())
		}
		dir.Close()
		dir = child
	}
	w, err := dir.OpenWrite(fileName, filesystem.CreateReuse(0o644))
	if err != nil {
		return err
	}
	defer w.Close()
	if _, err := w.WriteAt(file.Contents, 0); err != nil {
		return err
	}
	return nil
}

type serverLogsDirectoryUploader struct {
	context                 context.Context
	executeResponse         *remoteexecution.ExecuteResponse
	digestFunction          digest.Function
	writableFileUploadDelay <-chan struct{}
}

func (u *serverLogsDirectoryUploader) uploadDirectory(parentDirectory UploadableDirectory, dName path.Component, dPath *path.Trace) {
	d, err := parentDirectory.EnterUploadableDirectory(dName)
	if err != nil {
		attachErrorToExecuteResponse(u.executeResponse, util.StatusWrapf(err, "Failed to enter server logs directory %#v", dPath.GetUNIXString()))
		return
	}
	defer d.Close()

	files, err := d.ReadDir()
	if err != nil {
		attachErrorToExecuteResponse(u.executeResponse, util.StatusWrapf(err, "Failed to read server logs directory %#v", dPath.GetUNIXString()))
		return
	}

	for _, file := range files {
		childName := file.Name()
		childPath := dPath.Append(childName)
		switch fileType := file.Type(); fileType {
		case filesystem.FileTypeRegularFile:
			if childDigest, err := d.UploadFile(u.context, childName, u.digestFunction, u.writableFileUploadDelay); err == nil {
				u.executeResponse.ServerLogs[childPath.GetUNIXString()] = &remoteexecution.LogFile{
					Digest: childDigest.GetProto(),
				}
			} else {
				attachErrorToExecuteResponse(u.executeResponse, util.StatusWrapf(err, "Failed to store server log %#v", childPath.GetUNIXString()))
			}
		case filesystem.FileTypeDirectory:
			u.uploadDirectory(d, childName, childPath)
		}
	}
}
