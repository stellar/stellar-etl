package cmd

import (
	"testing"
)

func TestExportContractEvents(t *testing.T) {
	tests := []cliTest{
		{
			name:    "diagnostic events from one ledger",
			args:    []string{"export_contract_events", "-s", "52271338", "-e", "52271350", "-o", gotTestDir(t, "one_ledger_txs.txt")},
			golden:  "one_ledger_txs.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/contract_events/")
	}
}
