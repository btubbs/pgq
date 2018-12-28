package pgq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAfter(t *testing.T) {
	j := &Job{}
	when := time.Now().Add(time.Minute)
	After(when)(j)
	assert.Equal(t, when, j.RunAfter)
}

func TestRetryWaits(t *testing.T) {
	j := &Job{}
	waits := []time.Duration{time.Minute, time.Minute * 60}
	RetryWaits(waits)(j)
	assert.Equal(t, Durations(waits), j.RetryWaits)
}
