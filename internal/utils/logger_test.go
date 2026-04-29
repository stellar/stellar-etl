package utils

import (
	"errors"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

// newExitRecorder returns a capture func for SetExitFunc and a pointer to its
// state, so tests can assert whether the logger would have exited the process.
func newExitRecorder() (func(int), *struct {
	called bool
	code   int
}) {
	rec := &struct {
		called bool
		code   int
	}{}
	return func(code int) {
		rec.called = true
		rec.code = code
	}, rec
}

func TestLogError_StrictModeCallsExitWithCode1(t *testing.T) {
	logger := NewEtlLogger()
	logger.StrictExport = true
	fn, rec := newExitRecorder()
	logger.SetExitFunc(fn)

	logger.LogError(errors.New("transform failed"))

	assert.True(t, rec.called, "expected exit to be called when StrictExport is true")
	assert.Equal(t, 1, rec.code, "expected exit code 1")
}

func TestLogError_NonStrictModeDoesNotExit(t *testing.T) {
	logger := NewEtlLogger()
	logger.StrictExport = false
	fn, rec := newExitRecorder()
	logger.SetExitFunc(fn)

	logger.LogError(errors.New("transform failed"))

	assert.False(t, rec.called, "expected exit NOT to be called when StrictExport is false")
}

// TestAddCommonFlags_StrictExportDefaultsToTrue locks in the fail-fast default:
// without any explicit --strict-export flag, transforms errors must be fatal.
func TestAddCommonFlags_StrictExportDefaultsToTrue(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	AddCommonFlags(fs)

	assert.NoError(t, fs.Parse(nil))

	got, err := fs.GetBool("strict-export")
	assert.NoError(t, err)
	assert.True(t, got, "strict-export should default to true so transform failures halt the run")
}
