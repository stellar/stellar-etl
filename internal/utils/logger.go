package utils

import "github.com/stellar/go/support/log"

type EtlLogger struct {
	*log.Entry
	StrictExport bool
}

func NewEtlLogger() *EtlLogger {
	return &EtlLogger{
		log.New(),
		false,
	}
}

func (l *EtlLogger) LogError(err error) {
	if l.StrictExport {
		l.Fatal(err) //nolint:typecheck
	} else {
		l.Error(err) //nolint:typecheck
	}
}
