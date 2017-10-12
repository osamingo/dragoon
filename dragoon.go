package dragoon

import (
	"regexp"
	"time"

	"github.com/pkg/errors"
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
	// CreateTimeStamper gives getter and setter for CreatedAt.
	CreateTimeStamper interface {
		GetCreatedAt() time.Time
		SetCreatedAt(time.Time)
	}
	// UpdateTimeStamper gives setter for UpdatedAt.
	UpdateTimeStamper interface {
		SetUpdatedAt(time.Time)
	}
	// IdentifyGenerator gives generate of ID method.
	IdentifyGenerator interface {
		NextID(c context.Context, kind string) (string, error)
	}
	// Validator gives validate of fields method.
	Validator interface {
		Struct(target interface{}) error
	}
	// Spear has convenience methods.
	Spear struct {
		namespace           string
		kind                string
		ignoreFieldMismatch bool
		identifyGenerator   IdentifyGenerator
		validator           Validator
	}
)

var (
	// ErrConflictEntity is returned when an entity was conflict for a given key.
	ErrConflictEntity = errors.New("dragoon: conflict entity")
	nsReg             = regexp.MustCompile(`^[0-9A-Za-z._-]{0,100}$`)
)

// NewSpear returns new Spear.
func NewSpear(namespace, kind string, ignoreFieldMismatch bool, i IdentifyGenerator, v Validator) (*Spear, error) {
	if kind == "" || i == nil || v == nil {
		return nil, errors.New("dragoon: invalid arguments")
	}
	if namespace != "" && !nsReg.MatchString(namespace) {
		return nil, errors.New("dragoon: invalid namespace")
	}
	return &Spear{
		namespace:           namespace,
		kind:                kind,
		ignoreFieldMismatch: ignoreFieldMismatch,
		identifyGenerator:   i,
		validator:           v,
	}, nil
}

// Get loads the entity based on e's key into e.
func (s *Spear) Get(c context.Context, e Identifier) error {
	c = s.SetNamespaceIfNotEmpty(c)
	k := datastore.NewKey(c, string(s.kind), e.GetID(), 0, nil)
	err := datastore.Get(c, k, e)
	if err != nil {
		if s.ignoreFieldMismatch && IsErrFieldMismatch(err) {
			return nil
		}
		return errors.Wrapf(err, "dragoon: failed to get an entity - key = %#v", k)
	}
	return nil
}

// GetMulti is a batch version of Get.
func (s *Spear) GetMulti(c context.Context, es []Identifier) error {
	c = s.SetNamespaceIfNotEmpty(c)
	ks := make([]*datastore.Key, len(es))
	for i := range es {
		ks[i] = datastore.NewKey(c, string(s.kind), es[i].GetID(), 0, nil)
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
		return errors.Wrapf(err, "dragoon: failed to get entities - keys = %#v", ks)
	}
	return nil
}

// Put saves the entity src into the datastore based on e's ID.
func (s *Spear) Put(c context.Context, e Identifier) error {
	c = s.SetNamespaceIfNotEmpty(c)
	if err := s.CheckID(c, e); err != nil {
		return errors.Wrap(err, "dragoon: failed to generate ID")
	}
	SetTimeStamps(e, Now())
	if err := s.validator.Struct(e); err != nil {
		return errors.Wrap(err, "dragoon: invalid validation")
	}
	k := datastore.NewKey(c, string(s.kind), e.GetID(), 0, nil)
	if _, err := datastore.Put(c, k, e); err != nil {
		return errors.Wrapf(err, "dragoon: failed to put an entity - key = %#v, entity = %#v", k, e)
	}
	return nil
}

// PutMulti is a batch version of Put.
func (s *Spear) PutMulti(c context.Context, es []Identifier) error {
	c = s.SetNamespaceIfNotEmpty(c)
	ks := make([]*datastore.Key, len(es))
	now := Now()
	for i := range es {
		if err := s.CheckID(c, es[i]); err != nil {
			return errors.Wrap(err, "dragoon: failed to generate new ID")
		}
		SetTimeStamps(es[i], now)
		if err := s.validator.Struct(es[i]); err != nil {
			return errors.Wrap(err, "dragoon: invalid validation")
		}
		ks[i] = datastore.NewKey(c, string(s.kind), es[i].GetID(), 0, nil)
	}
	if _, err := datastore.PutMulti(c, ks, es); err != nil {
		return errors.Wrapf(err, "dragoon: failed to put entities - keys = %#v, entities = %#v", ks, es)
	}
	return nil
}

// Delete deletes the entity for the given Identifier.
func (s *Spear) Delete(c context.Context, e Identifier) error {
	c = s.SetNamespaceIfNotEmpty(c)
	k := datastore.NewKey(c, string(s.kind), e.GetID(), 0, nil)
	if err := datastore.Delete(c, k); err != nil {
		return errors.Wrapf(err, "dragoon: failed to delete an entity - key = %#v", k)
	}
	return nil
}

// DeleteMulti is a batch version of Delete.
func (s *Spear) DeleteMulti(c context.Context, es []Identifier) error {
	c = s.SetNamespaceIfNotEmpty(c)
	ks := make([]*datastore.Key, len(es))
	for i := range es {
		ks[i] = datastore.NewKey(c, string(s.kind), es[i].GetID(), 0, nil)
	}
	if err := datastore.DeleteMulti(c, ks); err != nil {
		return errors.Wrapf(err, "dragoon: failed to delete entities - keys = %#v", ks)
	}
	return nil
}

// Save saves the entity src into the datastore based on e's ID after checks exist an entity based e's ID.
func (s *Spear) Save(c context.Context, e Identifier) error {
	c = s.SetNamespaceIfNotEmpty(c)
	if err := s.CheckID(c, e); err != nil {
		return errors.Wrap(err, "dragoon: failed to generate ID")
	}
	SetTimeStamps(e, Now())
	if err := s.validator.Struct(e); err != nil {
		return errors.Wrap(err, "dragoon: invalid validation")
	}
	return datastore.RunInTransaction(c, func(tc context.Context) error {
		k := datastore.NewKey(tc, string(s.kind), e.GetID(), 0, nil)
		err := datastore.Get(tc, k, e)
		switch err {
		case nil:
			return ErrConflictEntity
		case datastore.ErrNoSuchEntity:
			_, err = datastore.Put(tc, k, e)
		}
		return err
	}, nil)
}

// CheckID checks e's ID. if e's ID is empty, set generated new ID.
func (s *Spear) CheckID(c context.Context, e Identifier) error {
	if id := e.GetID(); id != "" {
		e.SetID(id)
		return nil
	}
	id, err := s.identifyGenerator.NextID(c, string(s.kind))
	if err != nil {
		return err
	}
	e.SetID(id)
	return nil
}

// SetNamespaceIfNotEmpty sets namespace to context if s.namespace not empty.
func (s *Spear) SetNamespaceIfNotEmpty(c context.Context) context.Context {
	if s.namespace != "" {
		c, _ = appengine.Namespace(c, s.namespace)
	}
	return c
}

// Now returns current mills time by UTC.
func Now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

// SetTimeStamps sets a time to xTimeStampers.
func SetTimeStamps(i interface{}, t time.Time) {
	if c, ok := i.(CreateTimeStamper); ok && c.GetCreatedAt().IsZero() {
		c.SetCreatedAt(t)
	}
	if u, ok := i.(UpdateTimeStamper); ok {
		u.SetUpdatedAt(t)
	}
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

// FillID fills es's ID fields.
func FillID(ks []*datastore.Key, es []Identifier) {
	for i := range ks {
		if ks[i] == nil {
			continue
		}
		es[i].SetID(ks[i].StringID())
	}
}

// AsMap converts a slice of identifiers to map.
func AsMap(is []Identifier) map[string]interface{} {
	m := make(map[string]interface{}, len(is))
	for i := range is {
		m[is[i].GetID()] = is[i]
	}
	return m
}
