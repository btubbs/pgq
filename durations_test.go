package pgq

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDurationsValue(t *testing.T) {

	tt := []struct {
		desc        string
		durations   Durations
		expectedVal driver.Value
		expectedErr error
	}{
		{
			desc:        "a few durations",
			durations:   Durations{time.Minute, time.Minute * 60 * 48, 10},
			expectedVal: "{1m0s,48h0m0s,10ns}",
		},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			val, err := tc.durations.Value()
			assert.Equal(t, tc.expectedVal, val)
			assert.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestDurationsScan(t *testing.T) {
	tt := []struct {
		desc              string
		src               interface{}
		hasErr            bool
		expectedDurations Durations
	}{
		{
			desc:              "valid src",
			src:               []byte("{1m0s,48h0m0s,10ns}"),
			expectedDurations: Durations{time.Minute, time.Minute * 60 * 48, 10},
		},
		{
			desc:              "nil src",
			expectedDurations: Durations{},
		},
		{
			desc:              "non byte src",
			src:               "blah blah",
			expectedDurations: Durations{},
			hasErr:            true,
		},
		{
			desc:              "invalid durations",
			src:               []byte("{blerg}"),
			expectedDurations: Durations{},
			hasErr:            true,
		},
	}
	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			ds := Durations{}
			err := ds.Scan(tc.src)
			assert.Equal(t, tc.expectedDurations, ds)
			assert.Equal(t, tc.hasErr, err != nil)
		})
	}
}
