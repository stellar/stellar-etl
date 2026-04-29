package cmd

import (
	"testing"
)

func TestExportEffects(t *testing.T) {
	tests := []CliTest{
		{
			Name:              "effects from one ledger",
			Args:              []string{"export_effects", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_effects/")},
			Golden:            "one_ledger_effects.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "effects from 10 ledgers",
			Args:              []string{"export_effects", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_effects/")},
			Golden:            "10_ledgers_effects.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "effects across multiple batches",
			Args:              []string{"export_effects", "-s", "30822015", "-e", "30822025", "-b", "3", "-o", GotTestDir(t, "multi_batch_effects/")},
			Golden:            "10_ledgers_effects.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "ledger with no effects",
			Args:              []string{"export_effects", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_effects/")},
			Golden:            "ledger_no_effects.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/effects/", "", false)
	}
}
