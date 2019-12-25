package require

import (
	"testing"

	"github.com/morikuni/redis/internal/assert"
)

func WantError(t testing.TB, want bool, err error) {
	t.Helper()

	if !assert.WantError(t, want, err) {
		t.FailNow()
	}
}
