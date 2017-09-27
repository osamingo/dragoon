package dragoon

import (
	"errors"
	"time"

	"github.com/mjibson/goon"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

type (
	// Identifier gives getter and setter for ID.
	Identifier interface {
		GetID() string
		SetID(string)
	}
	// TimeStamper gives getter for CreatedAt, setter for CreatedAt and UpdatedAt.
	TimeStamper interface {
		GetCreatedAt() time.Time
		SetCreatedAt(time.Time)
		SetUpdatedAt(time.Time)
	}
	// IdentifyGenerator gives generate of ID method.
	IdentifyGenerator interface {
		Generate(context.Context) (string, error)
	}
	// Validator gives validate of fields method.
	Validator interface {
		Validate(context.Context, interface{}) error
	}
	// Spear has convenience methods with mjibson/goon.
	Spear struct {
		ig IdentifyGenerator
		v  Validator
	}
)

// NewSpear returns new Spear.
func NewSpear(ig IdentifyGenerator, v Validator) (*Spear, error) {
	if ig == nil {
		return nil, errors.New("dragoon: invalid argument - IdentifyGenerator should not be nil")
	}
	if v == nil {
		return nil, errors.New("dragoon: invalid argument - Validator should not be nil")
	}
	return &Spear{
		ig: ig,
		v:  v,
	}, nil
}

// Get loads the entity based on dst's key into dst
// If there is no such entity for the key, Get returns
// datastore.ErrNoSuchEntity.
func (s *Spear) Get(c context.Context, e interface{}) error {
	return goon.FromContext(c).Get(e)
}

// GetMulti is a batch version of Get.
func (s *Spear) GetMulti(c context.Context, es interface{}) error {
	return goon.FromContext(c).GetMulti(es)
}

// Count returns the number of results for the query.
func (s *Spear) Count(c context.Context, q *datastore.Query) (int, error) {
	return goon.FromContext(c).Count(q)
}

// GetAll runs the query and returns all the keys that match the query, as well
// as appending the values to dst, setting the goon key fields of dst, and
// caching the returned data in local memory.
func (s *Spear) GetAll(c context.Context, q *datastore.Query, es interface{}) error {
	_, err := goon.FromContext(c).GetAll(q, es)
	return err
}

// RunInTransaction runs f in a transaction.
func (s *Spear) RunInTransaction(c context.Context, f func(g *goon.Goon) error, o *datastore.TransactionOptions) error {
	return goon.FromContext(c).RunInTransaction(f, o)
}

// FlushLocalCache clears the local memory cache.
func (s *Spear) FlushLocalCache(c context.Context) {
	goon.FromContext(c).FlushLocalCache()
}

// Delete deletes the entity for the given goon entity with kind and id.
func (s *Spear) Delete(c context.Context, e interface{}) error {
	g := goon.FromContext(c)
	return g.Delete(g.Key(e))
}

// DeleteMulti is a batch version of Delete.
func (s *Spear) DeleteMulti(c context.Context, es ...interface{}) error {
	g := goon.FromContext(c)
	ks := make([]*datastore.Key, len(es))
	for i := range es {
		ks[i] = g.Key(es[i])
	}
	return g.DeleteMulti(ks)
}

// Put saves the entity src into the datastore based on e's key k.
func (s *Spear) Put(c context.Context, e interface{}) error {
	if i, ok := e.(Identifier); ok {
		if err := s.SetID(c, i); err != nil {
			return err
		}
	}
	if ts, ok := e.(TimeStamper); ok {
		s.SetTimeStamps(ts, s.Now())
	}
	if err := s.v.Validate(c, e); err != nil {
		return err
	}
	_, err := goon.FromContext(c).Put(e)
	return err
}

// PutMulti is a batch version of Put.
func (s *Spear) PutMulti(c context.Context, es ...interface{}) error {
	now := s.Now()
	for i := range es {
		if id, ok := es[i].(Identifier); ok {
			if err := s.SetID(c, id); err != nil {
				return err
			}
		}
		if ts, ok := es[i].(TimeStamper); ok {
			s.SetTimeStamps(ts, now)
		}
		if err := s.v.Validate(c, es[i]); err != nil {
			return err
		}
	}
	_, err := goon.FromContext(c).PutMulti(es)
	return err
}

// SetID sets ID. if ID is empty, set generated ID.
func (s *Spear) SetID(c context.Context, e Identifier) error {
	id := e.GetID()
	if id != "" {
		e.SetID(id)
		return nil
	}
	newID, err := s.ig.Generate(c)
	if err != nil {
		return err
	}
	e.SetID(newID)
	return nil
}

// Now returns current mills time by UTC.
func (s *Spear) Now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

// SetTimeStamps sets a time to TimeStamper.
func (s *Spear) SetTimeStamps(ts TimeStamper, t time.Time) {
	if ts.GetCreatedAt().IsZero() {
		ts.SetCreatedAt(t)
	}
	ts.SetUpdatedAt(t)
}
