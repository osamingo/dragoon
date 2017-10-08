package identifier

import (
	"encoding/hex"

	"github.com/google/uuid"
	"golang.org/x/net/context"
)

// Random gives hex id by UUID v4.
type Random struct{}

// NextID implements dragoon.Identifier.
func (Random) NextID(context.Context, string) (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}
