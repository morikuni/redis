package assert

import (
	"reflect"
	"testing"
)

func Equal(t testing.TB, want, got interface{}) bool {
	t.Helper()

	if reflect.DeepEqual(want, got) {
		return true
	}

	t.Errorf("not equal\n  want:%#v\n  got: %#v", want, got)
	return false
}

func WantError(t testing.TB, want bool, err error) bool {
	t.Helper()

	switch {
	case want && err == nil:
		t.Error("want error but nil")
		return false
	case !want && err != nil:
		t.Errorf("unexpected error: %v", err)
		return false
	default:
		return true
	}
}
