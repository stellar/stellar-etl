package cmd

import (
	"testing"
)

func TestExportOperations(t *testing.T) {
	tests := []CliTest{
		{
			Name:              "operations from one ledger",
			Args:              []string{"export_operations", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_ops/")},
			Golden:            "one_ledger_ops.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "operations from 10 ledgers",
			Args:              []string{"export_operations", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_ops/")},
			Golden:            "10_ledgers_ops.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "operations across multiple batches",
			Args:              []string{"export_operations", "-s", "30822015", "-e", "30822025", "-b", "3", "-o", GotTestDir(t, "multi_batch_ops/")},
			Golden:            "10_ledgers_ops.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "ledger with no operations",
			Args:              []string{"export_operations", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_ops/")},
			Golden:            "ledger_no_ops.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/operations/", "", false)
	}
}
