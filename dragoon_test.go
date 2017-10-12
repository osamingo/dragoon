package dragoon

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/osamingo/dragoon/identifier"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
	"gopkg.in/go-playground/validator.v9"
)

type Entity struct {
	ID          string    `datastore:"-" valid:"min=1,max=11"`
	Name        string    `datastore:"name" valid:"required"`
	Description string    `datastore:"description,omitempty,noindex" valid:"max=140"`
	CreatedAt   time.Time `datastore:"created_at"`
	UpdatedAt   time.Time `datastore:"updated_at"`
}

var (
	inst aetest.Instance
	s    *Spear
)

func (e *Entity) GetID() string {
	return e.ID
}

func (e *Entity) SetID(id string) {
	e.ID = id
}

func (e *Entity) GetCreatedAt() time.Time {
	return e.CreatedAt
}

func (e *Entity) SetCreatedAt(t time.Time) {
	e.CreatedAt = t
}

func (e *Entity) SetUpdatedAt(t time.Time) {
	e.UpdatedAt = t
}

func TestMain(m *testing.M) {
	var e interface{} = &Entity{}
	if _, ok := e.(Identifier); !ok {
		fmt.Fprint(os.Stderr, "Identifier not implemented")
		os.Exit(1)
	}
	if _, ok := e.(CreateTimeStamper); !ok {
		fmt.Fprint(os.Stderr, "CreateTimeStamper not implemented")
		os.Exit(1)
	}
	if _, ok := e.(UpdateTimeStamper); !ok {
		fmt.Fprint(os.Stderr, "UpdateTimeStamper not implemented")
		os.Exit(1)
	}
	os.Exit(run(m))
}

func run(m *testing.M) int {

	var err error
	s, err = NewSpear("test-ns", "test-kind", true, identifier.DatastoreAllocate{}, validator.New())
	if err != nil {
		fmt.Fprint(os.Stderr, "failed to generate spear - error =", err.Error())
		os.Exit(1)
	}

	inst, err = aetest.NewInstance(&aetest.Options{
		AppID: "dragoon-test",
		StronglyConsistentDatastore: true,
	})
	if err != nil {
		fmt.Fprint(os.Stderr, "failed to generate test instance - error =", err.Error())
		os.Exit(1)
	}
	defer inst.Close()

	return m.Run()
}

func newTestContext() (context.Context, error) {
	req, err := inst.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		return nil, err
	}
	return appengine.NewContext(req), nil
}

func TestNewSpear(t *testing.T) {
	_, err := NewSpear("", "", false, identifier.DatastoreAllocate{}, validator.New())
	require.Error(t, err)
	_, err = NewSpear("+", "test", false, identifier.DatastoreAllocate{}, validator.New())
	require.Error(t, err)
	s, err = NewSpear("", "test", false, identifier.DatastoreAllocate{}, validator.New())
	require.NoError(t, err)
	assert.NotNil(t, s)
}

func TestSpear(t *testing.T) {

	c, err := newTestContext()
	require.NoError(t, err)

	src := &Entity{
		Name: "Single_1",
	}
	require.NoError(t, s.Put(c, src))
	require.EqualError(t, s.Save(c, src), ErrConflictEntity.Error())

	dst := &Entity{
		ID: src.ID,
	}
	require.NoError(t, s.Get(c, dst))
	assert.EqualValues(t, src, dst)

	require.NoError(t, s.Delete(c, dst))

	require.NoError(t, s.Save(c, src))
	require.NoError(t, s.Delete(c, dst))
}

func TestSpearMulti(t *testing.T) {

	c, err := newTestContext()
	require.NoError(t, err)

	src := []Identifier{
		&Entity{
			Name: "Multi_1",
		},
		&Entity{
			Name: "Multi_2",
		},
	}

	require.NoError(t, s.PutMulti(c, src))

	id1 := src[0].(Identifier).GetID()
	id2 := src[1].(Identifier).GetID()
	dst := []Identifier{
		&Entity{
			ID: id1,
		},
		&Entity{
			ID: id2,
		},
	}
	require.NoError(t, s.GetMulti(c, dst))
	assert.EqualValues(t, src[0], dst[0])
	assert.EqualValues(t, src[1], dst[1])

	require.NoError(t, s.DeleteMulti(c, dst))

	err = s.GetMulti(c, []Identifier{
		&Entity{
			ID: id1,
		},
		&Entity{
			ID: id2,
		},
	})
	require.EqualError(t, errors.Cause(err).(appengine.MultiError)[0], datastore.ErrNoSuchEntity.Error())
	require.EqualError(t, errors.Cause(err).(appengine.MultiError)[1], datastore.ErrNoSuchEntity.Error())
}

func TestIsNotFound(t *testing.T) {
	require.True(t, IsNotFound(datastore.ErrNoSuchEntity))
}

func TestFillID(t *testing.T) {

	c, err := newTestContext()
	require.NoError(t, err)

	ks := []*datastore.Key{
		nil,
		datastore.NewKey(c, "test", "test-id", 0, nil),
	}
	es := []Identifier{
		&Entity{},
		&Entity{},
	}
	FillID(ks, es)

	assert.Empty(t, es[0].GetID())
	assert.Equal(t, "test-id", es[1].GetID())
}

func TestAsMap(t *testing.T) {

	is := []Identifier{
		&Entity{
			ID: "item1",
		},
		&Entity{
			ID: "item2",
		},
	}

	m := AsMap(is)
	require.NotNil(t, m)

	assert.Equal(t, "item1", m["item1"].(*Entity).GetID())
	assert.Equal(t, "item2", m["item2"].(*Entity).GetID())
}
