package identifier

import (
	"time"

	"github.com/howeyc/crc16"
	"github.com/osamingo/indigo"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
)

// Indigo has indigo.Generator
type Indigo struct {
	g *indigo.Generator
}

// NewIndigo returns Indigo.
func NewIndigo(start time.Time) *Indigo {
	return &Indigo{
		g: indigo.New(indigo.Settings{
			StartTime: start,
			MachineID: func() (uint16, error) {
				return crc16.ChecksumCCITT([]byte(appengine.InstanceID())), nil
			},
		}),
	}
}

// NextID implements dragoon.Identifier.
func (i *Indigo) NextID(context.Context, string) (string, error) {
	return i.g.NextID()
}
