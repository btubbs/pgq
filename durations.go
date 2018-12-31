package pgq

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"strings"
	"time"

	"github.com/joomcode/errorx"
)

// Durations is our own alias for time.Duration slices, so we can stick Scan and Value methods on them.  End
// users shouldn't have to use this type at all.
type Durations []time.Duration

// Value marshals the slice the Postgres syntax for a TEXT[] literal.
func (ds Durations) Value() (driver.Value, error) {
	stringVals := []string{}
	for _, d := range ds {
		stringVals = append(stringVals, d.String())
	}

	// putting these unquoted strings into this literal without quotes is safe because we know we got
	// these from time.Duration.String(), which does not use commas or curly braces.
	return "{" + strings.Join(stringVals, ",") + "}", nil
}

// Scan converts raw db bytes into Durations.  Nulls will be ignored.
func (ds *Durations) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	source, ok := src.([]byte)
	if !ok {
		return errors.New("source failed type assertion to []byte")
	}
	durs := []time.Duration{}
	// drop leading and trailing curly braces
	source = source[1 : len(source)-1]
	if len(source) == 0 {
		return nil
	}
	// loop over strings, parse them, and add to list
	parts := bytes.Split(source, []byte(","))
	for _, p := range parts {
		d, err := time.ParseDuration(string(p))
		if err != nil {
			return errorx.Decorate(err, "could not scan Durations value")
		}
		durs = append(durs, d)
	}
	newDS := Durations(durs)
	*ds = newDS
	return nil
}
