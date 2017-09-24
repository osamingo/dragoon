package dragoon

import (
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
	Spear struct {
		GenerateID func(context.Context) (string, error)
		Validation func(context.Context, interface{}) error
	}
)

func (d *Spear) Get(c context.Context, e interface{}) error {
	return goon.FromContext(c).Get(e)
}

func (d *Spear) GetMulti(c context.Context, es interface{}) error {
	return goon.FromContext(c).GetMulti(es)
}

func (d *Spear) Count(c context.Context, q *datastore.Query) (int, error) {
	return goon.FromContext(c).Count(q)
}

func (d *Spear) GetAll(c context.Context, q *datastore.Query, es interface{}) error {
	_, err := goon.FromContext(c).GetAll(q, es)
	return err
}

func (d *Spear) RunInTransaction(c context.Context, f func(tg *goon.Goon) error, opts *datastore.TransactionOptions) error {
	return goon.FromContext(c).RunInTransaction(f, opts)
}

func (d *Spear) FlushLocalCache(c context.Context) {
	goon.FromContext(c).FlushLocalCache()
}

func (d *Spear) Delete(c context.Context, e interface{}) error {
	g := goon.FromContext(c)
	return g.Delete(g.Key(e))
}

func (d *Spear) DeleteMulti(c context.Context, es ...interface{}) error {
	g := goon.FromContext(c)
	ks := make([]*datastore.Key, len(es))
	for i := range es {
		ks[i] = g.Key(es[i])
	}
	return g.DeleteMulti(ks)
}

func (d *Spear) Put(c context.Context, e interface{}) error {
	if id, ok := e.(Identifier); ok {
		if err := d.SetID(c, id); err != nil {
			return err
		}
	}
	if ts, ok := e.(TimeStamper); ok {
		d.SetTimeStamps(ts, d.Now())
	}
	if d.Validation != nil {
		if err := d.Validation(c, e); err != nil {
			return err
		}
	}
	_, err := goon.FromContext(c).Put(e)
	return err
}

func (d *Spear) PutMulti(c context.Context, es ...interface{}) error {
	now := d.Now()
	valid := d.Validation != nil
	for i := range es {
		if id, ok := es[i].(Identifier); ok {
			if err := d.SetID(c, id); err != nil {
				return err
			}
		}
		if ts, ok := es[i].(TimeStamper); ok {
			d.SetTimeStamps(ts, now)
		}
		if valid {
			if err := d.Validation(c, es[i]); err != nil {
				return err
			}
		}
	}
	_, err := goon.FromContext(c).PutMulti(es)
	return err
}

func (d *Spear) SetID(c context.Context, e Identifier) error {
	id := e.GetID()
	if id != "" {
		e.SetID(id)
		return nil
	}
	newID, err := d.GenerateID(c)
	if err != nil {
		return err
	}
	e.SetID(newID)
	return nil
}

func (d *Spear) Now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

func (d *Spear) SetTimeStamps(ts TimeStamper, t time.Time) {
	if ts.GetCreatedAt().IsZero() {
		ts.SetCreatedAt(t)
	}
	ts.SetUpdatedAt(t)
}
