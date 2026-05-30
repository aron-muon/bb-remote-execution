package egressauth_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/builder/egressauth"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestExtractGrant(t *testing.T) {
	mustHeader := func(name, value string) *anypb.Any {
		a, err := anypb.New(&remoteworker.ForwardedRequestHeader{Name: name, Value: value})
		require.NoError(t, err)
		return a
	}
	// An auxiliary_metadata entry of a different message type, which must
	// be skipped rather than mistaken for a header.
	other, err := anypb.New(&emptypb.Empty{})
	require.NoError(t, err)

	aux := []*anypb.Any{
		other,
		mustHeader("x-other", "ignored"),
		mustHeader(egressauth.DefaultGrantHeaderName, "signed.jwt.grant"),
	}

	t.Run("Present", func(t *testing.T) {
		grant, ok := egressauth.ExtractGrant(aux, egressauth.DefaultGrantHeaderName)
		require.True(t, ok)
		require.Equal(t, "signed.jwt.grant", grant)
	})
	t.Run("HeaderNameAbsent", func(t *testing.T) {
		_, ok := egressauth.ExtractGrant(aux, "no-such-header")
		require.False(t, ok)
	})
	t.Run("EmptyValueTreatedAsAbsent", func(t *testing.T) {
		empty := []*anypb.Any{mustHeader(egressauth.DefaultGrantHeaderName, "")}
		_, ok := egressauth.ExtractGrant(empty, egressauth.DefaultGrantHeaderName)
		require.False(t, ok)
	})
	t.Run("NoMetadata", func(t *testing.T) {
		_, ok := egressauth.ExtractGrant(nil, egressauth.DefaultGrantHeaderName)
		require.False(t, ok)
	})
}
