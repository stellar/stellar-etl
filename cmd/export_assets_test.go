package cmd

import (
	"testing"
)

func TestExportAssets(t *testing.T) {
	tests := []CliTest{
		{
			Name:              "assets from one ledger",
			Args:              []string{"export_assets", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_assets/")},
			Golden:            "one_ledger_assets.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "assets from 10 ledgers",
			Args:              []string{"export_assets", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_assets/")},
			Golden:            "10_ledgers_assets.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "assets across multiple batches",
			Args:              []string{"export_assets", "-s", "30822015", "-e", "30822025", "-b", "3", "-o", GotTestDir(t, "multi_batch_assets/")},
			Golden:            "10_ledgers_assets.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "ledger with no assets",
			Args:              []string{"export_assets", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_assets/")},
			Golden:            "ledger_no_assets.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/assets/", "", false)
	}
}
