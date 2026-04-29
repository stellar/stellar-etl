package cmd

import (
	"testing"
)

func TestExportContractEvents(t *testing.T) {
	tests := []CliTest{
		{
			Name:              "contract events from multiple ledger",
			Args:              []string{"export_contract_events", "-s", "52271338", "-e", "52271350", "-o", GotTestDir(t, "large_range_ledger_txs/")},
			Golden:            "large_range_ledger_txs.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "contract events across multiple batches",
			Args:              []string{"export_contract_events", "-s", "52271338", "-e", "52271350", "-b", "4", "-o", GotTestDir(t, "multi_batch_contract_events/")},
			Golden:            "large_range_ledger_txs.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/contract_events/", "", false)
	}
}
