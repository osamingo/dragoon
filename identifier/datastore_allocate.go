package identifier

import (
	"github.com/osamingo/indigo/base58"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

// DatastoreAllocate gives id by datastore allocate.
type DatastoreAllocate struct{}

// NextID implements dragoon.Identifier.
func (DatastoreAllocate) NextID(c context.Context, kind string) (string, error) {
	id, _, err := datastore.AllocateIDs(c, kind, nil, 1)
	if err != nil {
		return "", err
	}
	return base58.StdEncoding.Encode(uint64(id)), nil
}
