package cmd

import "testing"

func TestExportChanges(t *testing.T) {
	tests := []cliTest{
		{
			name:    "small range",
			args:    []string{"export_ledger_entry_changes", "-x", "../stellar-core/src/stellar-core -c", "../stellar-core/docs/stellar-core_example.cfg", "-s", "100", "-e", "101"},
			golden:  "bucket_read_exact.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/changes/")
	}
}
