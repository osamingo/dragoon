package dragoon

import (
	"errors"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
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
		Generate(c context.Context, kind string) (string, error)
	}
	// Validator gives validate of fields method.
	Validator interface {
		Validate(c context.Context, src interface{}) error
	}
	// Spear has convenience methods with mjibson/goon.
	Spear struct {
		kind                string
		ignoreFieldMismatch bool
		identifyGenerator   IdentifyGenerator
		validator           Validator
	}
)

// NewSpear returns new Spear.
func NewSpear(kind string, ignoreFieldMismatch bool, i IdentifyGenerator, v Validator) (*Spear, error) {
	if kind == "" {
		return nil, errors.New("dragoon: invalid argument - kind should not be empty")
	}
	if i == nil {
		return nil, errors.New("dragoon: invalid argument - IdentifyGenerator should not be nil")
	}
	if v == nil {
		return nil, errors.New("dragoon: invalid argument - Validator should not be nil")
	}
	return &Spear{
		kind:                kind,
		ignoreFieldMismatch: ignoreFieldMismatch,
		identifyGenerator:   i,
		validator:           v,
	}, nil
}

// Get loads the entity based on e's key into e.
func (s *Spear) Get(c context.Context, e Identifier) error {
	err := datastore.Get(c, datastore.NewKey(c, s.kind, e.GetID(), 0, nil), e)
	if err != nil {
		if s.ignoreFieldMismatch && IsErrFieldMismatch(err) {
			return nil
		}
		return err
	}
	return nil
}

// GetMulti is a batch version of Get.
func (s *Spear) GetMulti(c context.Context, es []Identifier) error {
	ks := make([]*datastore.Key, 0, len(es))
	for i := range es {
		ks = append(ks, datastore.NewKey(c, s.kind, es[i].GetID(), 0, nil))
	}
	err := datastore.GetMulti(c, ks, es)
	if err != nil {
		if me, ok := err.(appengine.MultiError); ok {
			for i := range me {
				if s.ignoreFieldMismatch && IsErrFieldMismatch(me[i]) {
					me[i] = nil
				}
			}
		}
		return err
	}
	return nil
}

// Put saves the entity src into the datastore based on e's ID.
func (s *Spear) Put(c context.Context, e Identifier) error {
	if err := s.SetID(c, e); err != nil {
		return err
	}
	if ts, ok := e.(TimeStamper); ok {
		SetTimeStamps(ts, Now())
	}
	if err := s.validator.Validate(c, e); err != nil {
		return err
	}
	_, err := datastore.Put(c, datastore.NewKey(c, s.kind, e.GetID(), 0, nil), e)
	return err
}

// PutMulti is a batch version of Put.
func (s *Spear) PutMulti(c context.Context, es []Identifier) error {
	now := Now()
	ks := make([]*datastore.Key, 0, len(es))
	for i := range es {
		if err := s.SetID(c, es[i]); err != nil {
			return err
		}
		if ts, ok := es[i].(TimeStamper); ok {
			SetTimeStamps(ts, now)
		}
		if err := s.validator.Validate(c, es[i]); err != nil {
			return err
		}
		ks = append(ks, datastore.NewKey(c, s.kind, es[i].GetID(), 0, nil))
	}
	_, err := datastore.PutMulti(c, ks, es)
	return err
}

// Delete deletes the entity for the given Identifier.
func (s *Spear) Delete(c context.Context, e Identifier) error {
	return datastore.Delete(c, datastore.NewKey(c, s.kind, e.GetID(), 0, nil))
}

// DeleteMulti is a batch version of Delete.
func (s *Spear) DeleteMulti(c context.Context, es []Identifier) error {
	ks := make([]*datastore.Key, 0, len(es))
	for i := range es {
		ks = append(ks, datastore.NewKey(c, s.kind, es[i].GetID(), 0, nil))
	}
	return datastore.DeleteMulti(c, ks)
}

// SetID sets ID. if ID is empty, set generated ID.
func (s *Spear) SetID(c context.Context, e Identifier) error {
	id := e.GetID()
	if id != "" {
		e.SetID(id)
		return nil
	}
	newID, err := s.identifyGenerator.Generate(c, s.kind)
	if err != nil {
		return err
	}
	e.SetID(newID)
	return nil
}

// Now returns current mills time by UTC.
func Now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

// SetTimeStamps sets a time to TimeStamper.
func SetTimeStamps(ts TimeStamper, t time.Time) {
	if ts.GetCreatedAt().IsZero() {
		ts.SetCreatedAt(t)
	}
	ts.SetUpdatedAt(t)
}

// IsErrFieldMismatch checks a type of datastore.ErrFieldMismatch or not.
func IsErrFieldMismatch(err error) bool {
	_, ok := err.(*datastore.ErrFieldMismatch)
	return ok
}

// IsNotFound checks it's datastore.ErrNoSuchEntity or not.
func IsNotFound(err error) bool {
	return err == datastore.ErrNoSuchEntity
}

// FillID fills id fields.
func FillID(ks []*datastore.Key, es []Identifier) {
	for i := range ks {
		if ks[i] == nil {
			continue
		}
		es[i].SetID(ks[i].StringID())
	}
}
