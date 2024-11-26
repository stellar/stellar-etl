package cmd

import (
	"testing"
)

func TestExportContractEvents(t *testing.T) {
	tests := []CliTest{
		{
			name:    "contract events from multiple ledger",
			args:    []string{"export_contract_events", "-s", "52271338", "-e", "52271350", "-o", GotTestDir(t, "large_range_ledger_txs.txt")},
			golden:  "large_range_ledger_txs.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/contract_events/")
	}
}
