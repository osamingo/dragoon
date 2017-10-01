package identifier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/appengine/aetest"
)

func TestDatastoreAllocate_NextID(t *testing.T) {

	c, done, err := aetest.NewContext()
	require.NoError(t, err)
	defer done()

	id, err := DatastoreAllocate{}.NextID(c, "testing")
	require.NoError(t, err)
	assert.NotEmpty(t, id)
}
