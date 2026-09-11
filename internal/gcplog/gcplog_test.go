package gcplog

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSeverity pins the level mapping, since these strings are what Cloud
// Logging filters on: a wrong one silently drops the line out of a
// severity>=ERROR query rather than failing loudly.
func TestSeverity(t *testing.T) {
	cases := map[logrus.Level]string{
		logrus.PanicLevel: "CRITICAL",
		logrus.FatalLevel: "CRITICAL",
		logrus.ErrorLevel: "ERROR",
		logrus.WarnLevel:  "WARNING",
		logrus.InfoLevel:  "INFO",
		logrus.DebugLevel: "DEBUG",
		logrus.TraceLevel: "DEBUG",
	}
	for level, want := range cases {
		assert.Equal(t, want, Severity(level), "level %s", level)
	}
}

// newTestLogger returns a logrus logger wired the way a service wires it, plus
// the buffer its lines land in.
func newTestLogger() (*logrus.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	logger := logrus.New()
	logger.SetOutput(buf)
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.AddHook(Hook{})
	return logger, buf
}

// decodeLines parses each non-empty output line as a JSON object.
func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var entry map[string]interface{}
		require.NoError(t, json.Unmarshal(line, &entry), "line is not JSON: %s", line)
		out = append(out, entry)
	}
	return out
}

func TestHookSetsSeverityAndMessage(t *testing.T) {
	logger, buf := newTestLogger()

	logger.Error("could not transform ledger 58231044")

	lines := decodeLines(t, buf)
	require.Len(t, lines, 1)
	assert.Equal(t, "ERROR", lines[0][SeverityField])
	assert.Equal(t, "could not transform ledger 58231044", lines[0][MessageField])
}

// TestHookDoesNotLeakBetweenLines guards the one risk in mutating Data from a
// hook. logrus copies the map per call, so a CRITICAL line must not leave its
// severity behind on the INFO line that follows.
func TestHookDoesNotLeakBetweenLines(t *testing.T) {
	logger, buf := newTestLogger()

	logger.Error("first")
	logger.Info("second")

	lines := decodeLines(t, buf)
	require.Len(t, lines, 2)
	assert.Equal(t, "ERROR", lines[0][SeverityField])
	assert.Equal(t, "first", lines[0][MessageField])
	assert.Equal(t, "INFO", lines[1][SeverityField])
	assert.Equal(t, "second", lines[1][MessageField])
}

// TestHookPreservesCallerFields checks that fields a service attaches survive
// alongside the mapped keys, which is what makes the context key usable.
func TestHookPreservesCallerFields(t *testing.T) {
	logger, buf := newTestLogger()

	logger.WithFields(logrus.Fields{
		ServiceField:   "stellar-etl",
		ComponentField: "export_ledgers",
		ContextField:   map[string]interface{}{"strict_export": true},
	}).Warn("deprecation notice")

	lines := decodeLines(t, buf)
	require.Len(t, lines, 1)
	assert.Equal(t, "WARNING", lines[0][SeverityField])
	assert.Equal(t, "stellar-etl", lines[0][ServiceField])
	assert.Equal(t, "export_ledgers", lines[0][ComponentField])
	assert.Equal(t, map[string]interface{}{"strict_export": true}, lines[0][ContextField])
}

// TestFormatterMapsFieldNames covers the path a service takes when it can set
// its own formatter, and so does not need the hook.
func TestFormatterMapsFieldNames(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := logrus.New()
	logger.SetOutput(buf)
	logger.SetFormatter(Formatter())

	logger.Error("boom")

	lines := decodeLines(t, buf)
	require.Len(t, lines, 1)
	assert.Equal(t, "error", lines[0][SeverityField], "formatter maps the raw logrus level, not the GCP name")
	assert.Equal(t, "boom", lines[0][MessageField])
	assert.NotContains(t, lines[0], "msg")
	assert.NotContains(t, lines[0], "level")
}

func TestParseLevel(t *testing.T) {
	t.Run("unset leaves the caller's default alone", func(t *testing.T) {
		// t.Setenv registers the restore; the Unsetenv is what the test needs.
		t.Setenv(LogLevelEnvVar, "")
		require.NoError(t, os.Unsetenv(LogLevelEnvVar))

		level, ok, err := ParseLevel()
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Zero(t, level)
	})

	t.Run("valid value is applied", func(t *testing.T) {
		t.Setenv(LogLevelEnvVar, "debug")
		level, ok, err := ParseLevel()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, logrus.DebugLevel, level)
	})

	t.Run("surrounding whitespace is tolerated", func(t *testing.T) {
		t.Setenv(LogLevelEnvVar, "  info\n")
		level, ok, err := ParseLevel()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, logrus.InfoLevel, level)
	})

	t.Run("bad value is reported rather than ignored", func(t *testing.T) {
		t.Setenv(LogLevelEnvVar, "loud")
		_, ok, err := ParseLevel()
		assert.Error(t, err)
		assert.False(t, ok)
	})
}
