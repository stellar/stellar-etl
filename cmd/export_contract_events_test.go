package cmd

import (
	"testing"
)

func TestExportContractEvents(t *testing.T) {
	tests := []cliTest{
		{
			name:    "contract events from multiple ledger",
			args:    []string{"export_contract_events", "-s", "52271338", "-e", "52271350", "-o", gotTestDir(t, "large_range_ledger_txs.txt")},
			golden:  "large_range_ledger_txs.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/contract_events/")
	}
}
