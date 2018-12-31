package pgq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackoff(t *testing.T) {
	b := Backoff("blah blah")
	assert.Equal(t, "blah blah", b.Error())
	assert.True(t, b.Backoff())
}
