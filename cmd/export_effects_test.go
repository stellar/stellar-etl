package cmd

import (
	"testing"
)

func TestExportEffects(t *testing.T) {
	tests := []cliTest{
		{
			name:    "effects from one ledger",
			args:    []string{"export_effects", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410100", "-o", gotTestDir(t, "one_ledger_effects.txt")},
			golden:  "one_ledger_effects.golden",
			wantErr: nil,
		},
		{
			name:    "effects from 10 ledgers",
			args:    []string{"export_effects", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110", "-o", gotTestDir(t, "10_ledgers_effects.txt")},
			golden:  "10_ledgers_effects.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_effects", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110", "-l", "5", "-o", gotTestDir(t, "large_range_effects.txt")},
			golden:  "large_range_effects.golden",
			wantErr: nil,
		},
		// disable for now since there's no ledger available in this backend with no effects
		// {
		// 	name:    "ledger with no effects",
		// 	args:    []string{"export_effects", "--testnet", "--gcs-bucket", "not", "-s", "??????", "-e", "??????", "-o", gotTestDir(t, "ledger_no_effects.txt")},
		// 	golden:  "ledger_no_effects.golden",
		// 	wantErr: nil,
		// },
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/effects/")
	}
}
