package cmd

import (
	"testing"
)

func TestExportDiagnosticEvents(t *testing.T) {
	t.Skip("Skipping due to unstable data in Futurenet")
	// TODO: find ledger with data and create testdata
	tests := []cliTest{
		{
			name:    "diagnostic events from one ledger",
			args:    []string{"export_diagnostic_events", "-s", "30820015", "-e", "30820015", "-o", gotTestDir(t, "one_ledger_txs.txt")},
			golden:  "one_ledger_txs.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/diagnostic_events/")
	}
}
