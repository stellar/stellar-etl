package cmd

import (
	"testing"
)

func TestExportEffects(t *testing.T) {
	tests := []CliTest{
		{
			Name:    "effects from one ledger",
			Args:    []string{"export_effects", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_effects.txt")},
			Golden:  "one_ledger_effects.golden",
			WantErr: nil,
		},
		{
			Name:    "effects from 10 ledgers",
			Args:    []string{"export_effects", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_effects.txt")},
			Golden:  "10_ledgers_effects.golden",
			WantErr: nil,
		},
		{
			Name:    "range too large",
			Args:    []string{"export_effects", "-s", "25820678", "-e", "25821678", "-l", "5", "-o", GotTestDir(t, "large_range_effects.txt")},
			Golden:  "large_range_effects.golden",
			WantErr: nil,
		},
		{
			Name:    "ledger with no effects",
			Args:    []string{"export_effects", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_effects.txt")},
			Golden:  "ledger_no_effects.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/effects/")
	}
}
