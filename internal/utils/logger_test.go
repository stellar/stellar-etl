package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-etl/v2/internal/gcplog"
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

// captureLogger returns a logger writing to a buffer instead of stdout, so a
// test can assert on the envelope a real run would emit.
func captureLogger(t *testing.T) (*EtlLogger, *bytes.Buffer) {
	t.Helper()
	logger := NewEtlLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)
	return logger, buf
}

// decodeOneLine parses the single JSON line the logger wrote.
func decodeOneLine(t *testing.T, buf *bytes.Buffer) map[string]interface{} {
	t.Helper()
	line := bytes.TrimSpace(buf.Bytes())
	require.NotEmpty(t, line, "logger wrote nothing")
	require.NotContains(t, string(line), "\n", "expected exactly one line")

	var entry map[string]interface{}
	require.NoError(t, json.Unmarshal(line, &entry), "line is not JSON: %s", line)
	return entry
}

// TestLogError_EmitsSharedEnvelope locks the log shape this service publishes.
// The keys are the contract the other services are meant to match, so a change
// here is a change to every query written against them, not a local detail.
func TestLogError_EmitsSharedEnvelope(t *testing.T) {
	logger, buf := captureLogger(t)
	logger.StrictExport = false
	logger.SetComponent("export_ledgers")

	logger.LogError(errors.New("could not transform ledger 58231044"))

	entry := decodeOneLine(t, buf)
	assert.Equal(t, "ERROR", entry[gcplog.SeverityField])
	assert.Equal(t, "could not transform ledger 58231044", entry[gcplog.MessageField])
	assert.Equal(t, serviceName, entry[gcplog.ServiceField])
	assert.Equal(t, "export_ledgers", entry[gcplog.ComponentField])
	assert.Equal(t,
		map[string]interface{}{"strict_export": false},
		entry[gcplog.ContextField],
		"service-specific detail belongs under context, not at the top level",
	)
}

// TestLogError_StrictModeSeverityIsCritical checks that a halting error is not
// reported at the same severity as one the export counted and carried past.
func TestLogError_StrictModeSeverityIsCritical(t *testing.T) {
	logger, buf := captureLogger(t)
	logger.StrictExport = true
	fn, _ := newExitRecorder()
	logger.SetExitFunc(fn)

	logger.LogError(errors.New("transform failed"))

	entry := decodeOneLine(t, buf)
	assert.Equal(t, "CRITICAL", entry[gcplog.SeverityField])
	assert.Equal(t, map[string]interface{}{"strict_export": true}, entry[gcplog.ContextField])
}

// TestSetDefaultLevel_YieldsToLogLevel covers the precedence that matters when
// someone is debugging a pod: an explicit LOG_LEVEL has to outlast the level a
// command sets for itself, or raising verbosity appears to do nothing.
func TestSetDefaultLevel_YieldsToLogLevel(t *testing.T) {
	t.Setenv(gcplog.LogLevelEnvVar, "debug")
	logger, buf := captureLogger(t)

	logger.SetDefaultLevel(logrus.InfoLevel)
	logger.Debug("still here")

	entry := decodeOneLine(t, buf)
	assert.Equal(t, "DEBUG", entry[gcplog.SeverityField])
}

// TestSetDefaultLevel_AppliesWithoutLogLevel is the other half: with no
// override, a command's own preferred level still takes effect.
func TestSetDefaultLevel_AppliesWithoutLogLevel(t *testing.T) {
	t.Setenv(gcplog.LogLevelEnvVar, "")
	require.NoError(t, os.Unsetenv(gcplog.LogLevelEnvVar))
	logger, buf := captureLogger(t)

	logger.SetDefaultLevel(logrus.InfoLevel)
	logger.Info("visible at info")
	logger.Debug("filtered out")

	entry := decodeOneLine(t, buf)
	assert.Equal(t, "INFO", entry[gcplog.SeverityField])
	assert.Equal(t, "visible at info", entry[gcplog.MessageField])
}

// TestSDKDefaultLoggerUsesSharedEnvelope covers the lines this binary does not
// write itself. The SDK packages (historyarchive above all) log through
// support/log's own instance, so if that one is missed it keeps emitting text on
// stderr and GKE keeps calling all of it ERROR, which is the exact problem this
// change exists to fix.
func TestSDKDefaultLoggerUsesSharedEnvelope(t *testing.T) {
	NewEtlLogger()

	buf := &bytes.Buffer{}
	log.DefaultLogger.SetOutput(buf)
	log.DefaultLogger.SetLevel(logrus.WarnLevel)

	log.Error("Error getting root HAS from archive")

	entry := decodeOneLine(t, buf)
	assert.Equal(t, "ERROR", entry[gcplog.SeverityField])
	assert.Equal(t, "Error getting root HAS from archive", entry[gcplog.MessageField])
	assert.Equal(t, serviceName, entry[gcplog.ServiceField])
	assert.Equal(t, sdkComponent, entry[gcplog.ComponentField],
		"SDK lines stay distinguishable from this binary's own")
}
