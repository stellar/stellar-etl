package cmd

import "testing"

func TestExportChanges(t *testing.T) {
	coreExecutablePath := "../stellar-core/src/stellar-core"
	coreConfigPath := "../stellar-core/docs/stellar-core_example.cfg"
	tests := []cliTest{
		{
			name:    "single ledger",
			args:    []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "100", "-e", "100", "--stdout"},
			golden:  "single_ledger.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/changes/")
	}
}
