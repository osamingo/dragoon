package dragoon

import (
	"errors"
	"time"

	"github.com/mjibson/goon"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

type (
	Identifier interface {
		GetID() string
		SetID(string)
	}
	TimeStamper interface {
		GetCreatedAt() time.Time
		SetCreatedAt(time.Time)
		SetUpdatedAt(time.Time)
	}
	IdentifyGenerator interface {
		Generate(context.Context) (string, error)
	}
	Validator interface {
		Validate(context.Context, interface{}) error
	}
	Spear struct {
		ig IdentifyGenerator
		v  Validator
	}
)

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

func (s *Spear) Get(c context.Context, e interface{}) error {
	return goon.FromContext(c).Get(e)
}

func (s *Spear) GetMulti(c context.Context, es interface{}) error {
	return goon.FromContext(c).GetMulti(es)
}

func (s *Spear) Count(c context.Context, q *datastore.Query) (int, error) {
	return goon.FromContext(c).Count(q)
}

func (s *Spear) GetAll(c context.Context, q *datastore.Query, es interface{}) error {
	_, err := goon.FromContext(c).GetAll(q, es)
	return err
}

func (s *Spear) RunInTransaction(c context.Context, f func(tg *goon.Goon) error, opts *datastore.TransactionOptions) error {
	return goon.FromContext(c).RunInTransaction(f, opts)
}

func (s *Spear) FlushLocalCache(c context.Context) {
	goon.FromContext(c).FlushLocalCache()
}

func (s *Spear) Delete(c context.Context, e interface{}) error {
	g := goon.FromContext(c)
	return g.Delete(g.Key(e))
}

func (s *Spear) DeleteMulti(c context.Context, es ...interface{}) error {
	g := goon.FromContext(c)
	ks := make([]*datastore.Key, len(es))
	for i := range es {
		ks[i] = g.Key(es[i])
	}
	return g.DeleteMulti(ks)
}

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

func (s *Spear) Now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

func (s *Spear) SetTimeStamps(ts TimeStamper, t time.Time) {
	if ts.GetCreatedAt().IsZero() {
		ts.SetCreatedAt(t)
	}
	ts.SetUpdatedAt(t)
}
