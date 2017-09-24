package dragoon

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/icrowley/fake"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
	"gopkg.in/go-playground/validator.v9"
)

type Entity struct {
	_kind       string    `datastore:"-" goon:"kind,TestKind"`
	ID          string    `datastore:"-" valid:"required" goon:"id" `
	Name        string    `datastore:"name" valid:"required"`
	Description string    `datastore:"description,omitempty,noindex" valid:"max=140"`
	CreatedAt   time.Time `datastore:"created_at"`
	UpdatedAt   time.Time `datastore:"updated_at"`
}

var d *Spear

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
	os.Exit(run(m))
}

func run(m *testing.M) int {

	err := os.Setenv("GAE_MODULE_INSTANCE", "test-instance-id")
	if err != nil {
		fmt.Fprint(os.Stderr, "failed to set environment value - error =", err.Error())
		os.Exit(1)
	}

	r := strings.NewReader(appengine.InstanceID())
	genid := func(context.Context) (string, error) {
		id, err := ulid.New(ulid.Now(), r)
		if err != nil {
			return "", err
		}
		return id.String(), nil
	}
	v := validator.New()
	valid := func(c context.Context, e interface{}) error {
		return v.StructCtx(c, e)
	}
	d = &Spear{
		GenerateID: genid,
		Validation: valid,
	}

	return m.Run()
}

func TestSpear(t *testing.T) {

	c, cancel, err := aetest.NewContext()
	require.NoError(t, err)
	defer cancel()

	src := &Entity{
		Name: fake.FullName(),
	}
	require.NoError(t, d.Put(c, src))

	dst := &Entity{
		ID: src.GetID(),
	}
	require.NoError(t, d.Get(c, dst))
	assert.EqualValues(t, src, dst)

	require.NoError(t, d.Delete(c, dst))
	require.EqualError(t, d.Get(c, &Entity{ID: dst.ID}), datastore.ErrNoSuchEntity.Error())
}

func TestSpear_Multi(t *testing.T) {

	c, cancel, err := aetest.NewContext()
	require.NoError(t, err)
	defer cancel()

	src := []interface{}{
		&Entity{
			Name: fake.FullName(),
		},
		&Entity{
			Name: fake.FullName(),
		},
	}
	require.NoError(t, d.PutMulti(c, src...))

	id1 := src[0].(Identifier).GetID()
	id2 := src[1].(Identifier).GetID()
	dst := []*Entity{
		{
			ID: id1,
		},
		{
			ID: id2,
		},
	}
	require.NoError(t, d.GetMulti(c, dst))
	assert.EqualValues(t, src[0], dst[0])
	assert.EqualValues(t, src[1], dst[1])

	require.NoError(t, d.DeleteMulti(c, []interface{}{dst[0], dst[1]}...))
	err = d.GetMulti(c, []*Entity{{ID: id1}, {ID: id2}})
	require.EqualError(t, err.(appengine.MultiError)[0], datastore.ErrNoSuchEntity.Error())
	require.EqualError(t, err.(appengine.MultiError)[1], datastore.ErrNoSuchEntity.Error())
}
