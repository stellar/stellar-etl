package cmd

import (
	"testing"
)

func TestExportTrades(t *testing.T) {
	tests := []CliTest{
		{
			Name:              "trades from one ledger",
			Args:              []string{"export_trades", "-s", "28770265", "-e", "28770265", "-o", GotTestDir(t, "one_ledger_trades/")},
			Golden:            "one_ledger_trades.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "trades from 10 ledgers",
			Args:              []string{"export_trades", "-s", "28770265", "-e", "28770275", "-o", GotTestDir(t, "10_ledgers_trades/")},
			Golden:            "10_ledgers_trades.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "trades across multiple batches",
			Args:              []string{"export_trades", "-s", "28770265", "-e", "28770275", "-b", "3", "-o", GotTestDir(t, "multi_batch_trades/")},
			Golden:            "10_ledgers_trades.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "ledger with no trades",
			Args:              []string{"export_trades", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_trades/")},
			Golden:            "ledger_no_trades.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/trades/", "", false)
	}
}
