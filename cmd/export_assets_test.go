package cmd

import (
	"testing"
)

func TestExportAssets(t *testing.T) {
	tests := []cliTest{
		{
			name:    "assets from one ledger",
			args:    []string{"export_assets", "-s", "30820015", "-e", "30820015", "--stdout"},
			golden:  "one_ledger_assets.golden",
			wantErr: nil,
		},
		{
			name:    "assets from 10 ledgers",
			args:    []string{"export_assets", "-s", "30822015", "-e", "30822025", "--stdout"},
			golden:  "10_ledgers_assets.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_assets", "-s", "30822015", "-e", "30822025", "-l", "5", "--stdout"},
			golden:  "large_range_assets.golden",
			wantErr: nil,
		},
		{
			name:    "ledger with no assets",
			args:    []string{"export_assets", "-s", "10363513", "-e", "10363513", "--stdout"},
			golden:  "ledger_no_assets.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/assets/")
	}
}
