package cmd

import (
	"testing"
)

func TestExportAssets(t *testing.T) {
	tests := []CliTest{
		{
			Name:    "assets from one ledger",
			Args:    []string{"export_assets", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_assets.txt")},
			Golden:  "one_ledger_assets.golden",
			WantErr: nil,
		},
		{
			Name:    "assets from 10 ledgers",
			Args:    []string{"export_assets", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_assets.txt")},
			Golden:  "10_ledgers_assets.golden",
			WantErr: nil,
		},
		{
			Name:    "range too large",
			Args:    []string{"export_assets", "-s", "30822015", "-e", "30822025", "-l", "5", "-o", GotTestDir(t, "large_range_assets.txt")},
			Golden:  "large_range_assets.golden",
			WantErr: nil,
		},
		{
			Name:    "ledger with no assets",
			Args:    []string{"export_assets", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_assets.txt")},
			Golden:  "ledger_no_assets.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/assets/")
	}
}
