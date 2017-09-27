package dragoon

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/icrowley/fake"
	"github.com/mjibson/goon"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
	"gopkg.in/go-playground/validator.v9"
)

type (
	Entity struct {
		_kind       string    `datastore:"-" goon:"kind,TestKind"`
		ID          string    `datastore:"-" valid:"len=26" goon:"id" `
		Name        string    `datastore:"name" valid:"required"`
		Description string    `datastore:"description,omitempty,noindex" valid:"max=140"`
		CreatedAt   time.Time `datastore:"created_at"`
		UpdatedAt   time.Time `datastore:"updated_at"`
	}
	IG struct {
		R io.Reader
	}
	V struct {
		V *validator.Validate
	}
)

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

func (ig *IG) Generate(context.Context) (string, error) {
	id, err := ulid.New(ulid.Now(), ig.R)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func (v *V) Validate(c context.Context, i interface{}) error {
	return v.V.StructCtx(c, i)
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

	s, err = NewSpear(&IG{R: strings.NewReader(appengine.InstanceID())}, &V{V: validator.New()})
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
	s, err := NewSpear(&IG{R: strings.NewReader(appengine.InstanceID())}, &V{V: validator.New()})
	require.NoError(t, err)
	assert.NotNil(t, s)
	_, err = NewSpear(nil, &V{V: validator.New()})
	require.Error(t, err)
	_, err = NewSpear(&IG{R: strings.NewReader(appengine.InstanceID())}, nil)
	require.Error(t, err)
}

func TestSpear(t *testing.T) {

	c, err := newTestContext()
	require.NoError(t, err)
	s.FlushLocalCache(c)

	src := &Entity{
		Name: fake.FullName(),
	}
	require.NoError(t, s.Put(c, src))

	cnt, err := s.Count(c, datastore.NewQuery(goon.FromContext(c).Kind(Entity{})))
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)

	dst := &Entity{
		ID: src.GetID(),
	}
	require.NoError(t, s.Get(c, dst))
	assert.EqualValues(t, src, dst)

	require.NoError(t, s.Delete(c, dst))
	err = s.RunInTransaction(c, func(g *goon.Goon) error {
		return s.Get(c, &Entity{ID: dst.ID})
	}, nil)
	require.EqualError(t, err, datastore.ErrNoSuchEntity.Error())
}

func TestSpear_Multi(t *testing.T) {

	c, err := newTestContext()
	require.NoError(t, err)
	s.FlushLocalCache(c)

	src := []interface{}{
		&Entity{
			Name: fake.FullName(),
		},
		&Entity{
			Name: fake.FullName(),
		},
	}

	require.NoError(t, s.PutMulti(c, src...))

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
	require.NoError(t, s.GetMulti(c, dst))
	assert.EqualValues(t, src[0], dst[0])
	assert.EqualValues(t, src[1], dst[1])

	dst = []*Entity{}
	err = s.GetAll(c, datastore.NewQuery(goon.FromContext(c).Kind(Entity{})), &dst)
	require.NoError(t, err)

	require.NoError(t, s.DeleteMulti(c, []interface{}{dst[0], dst[1]}...))
	err = s.GetMulti(c, []*Entity{{ID: id1}, {ID: id2}})
	require.EqualError(t, err.(appengine.MultiError)[0], datastore.ErrNoSuchEntity.Error())
	require.EqualError(t, err.(appengine.MultiError)[1], datastore.ErrNoSuchEntity.Error())
}
