package cmd

import (
	"testing"
)

func TestExportAssets(t *testing.T) {
	tests := []cliTest{
		{
			name:    "assets from one ledger",
			args:    []string{"export_assets", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410100", "-o", gotTestDir(t, "one_ledger_assets.txt")},
			golden:  "one_ledger_assets.golden",
			wantErr: nil,
		},
		{
			name:    "assets from 10 ledgers",
			args:    []string{"export_assets", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110", "-o", gotTestDir(t, "10_ledgers_assets.txt")},
			golden:  "10_ledgers_assets.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_assets", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110", "-l", "5", "-o", gotTestDir(t, "large_range_assets.txt")},
			golden:  "large_range_assets.golden",
			wantErr: nil,
		},
		// disable for now since there's no ledger available in this backend with no transactions
		// {
		// 	name:    "ledger with no assets",
		// 	args:    []string{"export_assets", "-s", "????", "-e", ""????", "-o", gotTestDir(t, "ledger_no_assets.txt")},
		// 	golden:  "ledger_no_assets.golden",
		// 	wantErr: nil,
		// },
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/assets/")
	}
}
