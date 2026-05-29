package egressauth

import (
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"

	"google.golang.org/protobuf/types/known/anypb"
)

// DefaultGrantHeaderName is the gRPC metadata header name under which the
// delegation grant is carried when the worker configuration does not
// specify one explicitly.
const DefaultGrantHeaderName = "bb-delegation-grant"

// ExtractGrant returns the delegation grant carried in a forwarded
// request header, if present.
//
// auxiliaryMetadata is DesiredState_Executing.AuxiliaryMetadata, into
// which the scheduler packs remoteworker.ForwardedRequestHeader messages
// for each allowlisted gRPC metadata header it forwards. headerName is
// the (lower-case) name of the header that carries the grant. The
// returned boolean reports whether a matching, non-empty grant header was
// found.
//
// When no matching header is present the caller is expected to fail open:
// run the action normally without routing it through the egress
// authentication sidecar.
func ExtractGrant(auxiliaryMetadata []*anypb.Any, headerName string) (string, bool) {
	for _, a := range auxiliaryMetadata {
		if !a.MessageIs((*remoteworker.ForwardedRequestHeader)(nil)) {
			continue
		}
		var header remoteworker.ForwardedRequestHeader
		if err := a.UnmarshalTo(&header); err != nil {
			continue
		}
		if header.GetName() == headerName && header.GetValue() != "" {
			return header.GetValue(), true
		}
	}
	return "", false
}
