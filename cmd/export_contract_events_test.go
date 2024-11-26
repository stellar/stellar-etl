package cmd

import (
	"testing"
)

func TestExportContractEvents(t *testing.T) {
	tests := []CliTest{
		{
			Name:    "contract events from multiple ledger",
			Args:    []string{"export_contract_events", "-s", "52271338", "-e", "52271350", "-o", GotTestDir(t, "large_range_ledger_txs.txt")},
			Golden:  "large_range_ledger_txs.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/contract_events/", "")
	}
}
