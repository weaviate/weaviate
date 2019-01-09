package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_EmptyFilters(t *testing.T) {
	result := New(nil).String()
	assert.Equal(t, "", result, "should return an empty string")
}
