package identifier

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndigo_NextID(t *testing.T) {

	os.Setenv("GAE_MODULE_INSTANCE", "00c61b117cb9258ad847fbaa07fbe892cfee822009338a056c07a2ae1b36594fdb3c1c501ee6fc914b65d1149b")

	i := NewIndigo(time.Unix(531976020, 0))
	require.NotNil(t, i)

	id, err := i.NextID(nil, "")
	require.NoError(t, err)
	require.NotEmpty(t, id)
}
