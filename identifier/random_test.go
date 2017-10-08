package identifier

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRandom_NextID(t *testing.T) {
	g := Random{}
	id, err := g.NextID(nil, "test")
	require.NoError(t, err)
	_, err = hex.DecodeString(id)
	require.NoError(t, err)
}
