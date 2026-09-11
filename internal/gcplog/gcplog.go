// Package gcplog renders logrus output as the structured JSON envelope the
// Stellar data platform emits from every service, in the shape Google Cloud
// Logging parses natively.
//
// Nothing here is specific to stellar-etl. The same envelope is meant to come
// out of the other Go exporters, the Python dlt pipelines and the dbt pods, so
// that one Logs Explorer filter reaches all of them. Wiring a new Go service
// means calling Configure with its name; nothing else in this package should
// need to change.
//
// The envelope:
//
//	{
//	  "severity":  "ERROR",                         // GCP LogSeverity, read natively
//	  "message":   "could not transform ledger 58231044",
//	  "service":   "stellar-etl",                   // which workload emitted the line
//	  "component": "export_ledgers",                // sub-unit: CLI subcommand, dbt node, dlt resource
//	  "context":   {"strict_export": true}          // service-specific, no cross-service meaning
//	}
//
// Only severity and message are interpreted by Cloud Logging; the rest are
// ordinary jsonPayload fields that queries can filter on. Keep the top level
// small. A key earns a place there only if it means the same thing in every
// service, which is what lets a filter span services without knowing who wrote
// the line. Anything that does not generalise belongs under context, where it
// stays queryable without claiming a shared meaning it does not have.
//
// Correlation back to Airflow is deliberately absent: Airflow already stamps
// dag_id, task_id, run_id and try_number as pod labels on every
// KubernetesPodOperator pod, so repeating them in the payload would only risk
// disagreeing with the labels.
package gcplog

import (
	"io"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

// Envelope keys. Exported so services can add to context, and so tests can
// assert on the shape without restating string literals.
const (
	SeverityField  = "severity"
	MessageField   = "message"
	ServiceField   = "service"
	ComponentField = "component"
	ContextField   = "context"
)

// LogLevelEnvVar overrides the level for a single run, so an operator can put a
// pod in debug from an Airflow variable rather than rebuilding the image.
const LogLevelEnvVar = "LOG_LEVEL"

// Severity maps a logrus level onto the LogSeverity strings Cloud Logging
// recognises. Panic and Fatal both collapse to CRITICAL: GCP has no level
// between ERROR and ALERT that means "the process is going down".
func Severity(level logrus.Level) string {
	switch level {
	case logrus.PanicLevel, logrus.FatalLevel:
		return "CRITICAL"
	case logrus.ErrorLevel:
		return "ERROR"
	case logrus.WarnLevel:
		return "WARNING"
	case logrus.InfoLevel:
		return "INFO"
	default:
		return "DEBUG"
	}
}

// Hook copies the level and message onto the two keys Cloud Logging reads when
// it ingests a JSON line.
//
// It exists because the field names cannot be set directly: the SDK's
// UseJSONFormatter builds a logrus.JSONFormatter with no FieldMap and exposes
// no way to replace the formatter, so logrus keeps emitting its own "level" and
// "msg". Those stay in the payload alongside the mapped keys. They are
// redundant but harmless, and this hook can be dropped if go-stellar-sdk ever
// accepts a field map.
type Hook struct{}

// Levels reports that the hook applies to every level, since every line needs a
// severity.
func (Hook) Levels() []logrus.Level { return logrus.AllLevels }

// Fire mutates the entry's Data map. logrus copies that map per call
// (logrus.Entry.Dup), so the write is scoped to one line and does not leak into
// the parent entry or across goroutines.
func (Hook) Fire(entry *logrus.Entry) error {
	entry.Data[SeverityField] = Severity(entry.Level)
	entry.Data[MessageField] = entry.Message
	return nil
}

// Formatter returns the JSON formatter this envelope expects, for services
// whose logger lets them set one directly. Services stuck behind a wrapper that
// hides the formatter get the same result from Hook.
func Formatter() logrus.Formatter {
	return &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyLevel: SeverityField,
			logrus.FieldKeyMsg:   MessageField,
		},
	}
}

// Output is the stream structured logs belong on.
//
// Cloud Logging derives severity from the stream for output it cannot parse, and
// GKE records everything on stderr as ERROR. That is why a workload logging
// plain text to stderr shows up as 100% ERROR no matter what it actually said.
// Writing every line to stdout hands the decision to the severity field instead.
func Output() io.Writer { return os.Stdout }

// ParseLevel resolves the LOG_LEVEL override. It reports ok=false when the
// variable is unset or unparseable, leaving the caller's default in place; err
// is non-nil only when a value was present and rejected, so a typo can be
// reported rather than silently ignored.
func ParseLevel() (level logrus.Level, ok bool, err error) {
	raw, present := os.LookupEnv(LogLevelEnvVar)
	if !present {
		return 0, false, nil
	}
	parsed, parseErr := logrus.ParseLevel(strings.TrimSpace(raw))
	if parseErr != nil {
		return 0, false, parseErr
	}
	return parsed, true, nil
}
