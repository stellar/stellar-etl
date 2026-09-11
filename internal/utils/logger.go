package utils

import (
	"github.com/sirupsen/logrus"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-etl/v2/internal/gcplog"
)

// serviceName identifies this binary in the shared log envelope. It is the one
// value a sibling exporter changes when it reuses internal/gcplog.
const serviceName = "stellar-etl"

type EtlLogger struct {
	*log.Entry
	StrictExport bool

	// levelPinned records that LOG_LEVEL set the level, so a command's own
	// preferred level cannot quietly undo an operator's override.
	levelPinned bool
}

// NewEtlLogger returns the logger every command shares. It emits one JSON
// object per line on stdout in the platform-wide envelope; see
// internal/gcplog for the shape and why stdout rather than stderr.
func NewEtlLogger() *EtlLogger {
	entry := log.New()
	entry.UseJSONFormatter()
	entry.SetOutput(gcplog.Output())
	entry.AddHook(gcplog.Hook{})

	logger := &EtlLogger{
		Entry:        entry.WithField(gcplog.ServiceField, serviceName),
		StrictExport: false,
	}

	// The level stays at the SDK default unless LOG_LEVEL asks otherwise. A
	// rejected value is reported rather than ignored, so a typo in an Airflow
	// variable does not look like a logger that silently did nothing.
	level, ok, err := gcplog.ParseLevel()
	switch {
	case err != nil:
		logger.Warnf("ignoring unparseable %s: %v", gcplog.LogLevelEnvVar, err)
	case ok:
		logger.SetLevel(level)
		logger.levelPinned = true
	}

	configureSDKDefaultLogger(level, ok)

	return logger
}

// sdkComponent labels lines the SDK emits on its own, so they can be told apart
// from this binary's without being hidden.
const sdkComponent = "go-stellar-sdk"

// configureSDKDefaultLogger puts the SDK's package-level logger in the same
// envelope as ours.
//
// support/log builds its own logrus instance in init(), and the SDK packages
// this binary pulls in log through that one rather than through the logger
// above: historyarchive alone has 13 package-level calls at WARNING or worse,
// including the archive read failures that matter most during an incident. Left
// alone they keep writing text to stderr, which GKE records as ERROR whatever
// the line actually said, so exactly the lines worth reading would have survived
// this change untouched.
//
// They carry service and component but never the running subcommand: this
// logger is a process global, configured before cobra has parsed anything.
func configureSDKDefaultLogger(level logrus.Level, levelPinned bool) {
	log.DefaultLogger.UseJSONFormatter()
	log.DefaultLogger.SetOutput(gcplog.Output())
	log.DefaultLogger.AddHook(gcplog.Hook{})
	log.DefaultLogger = log.DefaultLogger.
		WithField(gcplog.ServiceField, serviceName).
		WithField(gcplog.ComponentField, sdkComponent)

	if levelPinned {
		log.DefaultLogger.SetLevel(level)
	}
}

// SetComponent stamps the running subcommand onto every subsequent line, so a
// query can separate export_ledgers from export_trades without matching on
// message text.
func (l *EtlLogger) SetComponent(component string) {
	l.Entry = l.Entry.WithField(gcplog.ComponentField, component)
}

// SetDefaultLevel applies a command's preferred level unless LOG_LEVEL already
// pinned one, so an explicit operator override outranks a built-in default.
func (l *EtlLogger) SetDefaultLevel(level logrus.Level) {
	if l.levelPinned {
		return
	}
	l.SetLevel(level)
}

// LogError reports a transform or export failure, fatally under --strict-export.
//
// strict_export rides along under the context key because it answers the first
// question a reader has about one of these lines: did this halt the export, or
// was it counted and carried on past? It sits in context rather than at the top
// level because it means nothing in a service that has no such flag.
func (l *EtlLogger) LogError(err error) {
	entry := l.WithField(gcplog.ContextField, map[string]interface{}{
		"strict_export": l.StrictExport,
	})
	if l.StrictExport {
		entry.Fatal(err) //nolint:typecheck
	} else {
		entry.Error(err) //nolint:typecheck
	}
}
