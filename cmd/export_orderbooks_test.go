package cmd

import (
	"fmt"
	"testing"
)

func TestExportOrderbooks(t *testing.T) {
	tests := []cliTest{
		{
			name:    "unbounded range with no config",
			args:    []string{"export_orderbooks", "-x", coreExecutablePath, "-s", "100000"},
			golden:  "",
			wantErr: fmt.Errorf("stellar-core needs a config file path when exporting ledgers continuously (endNum = 0)"),
		},
		{
			name:    "0 batch size",
			args:    []string{"export_orderbooks", "-b", "0", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "100000", "-e", "164000"},
			golden:  "",
			wantErr: fmt.Errorf("batch-size (0) must be greater than 0"),
		},
		{
			name:              "orderbook from single ledger",
			args:              []string{"export_orderbooks", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "5000000", "-e", "5000000", "-o", gotTestDir(t, "single/")},
			golden:            "single_ledger.golden",
			sortForComparison: true,
			wantErr:           nil,
		},
		{
			name:              "orderbooks from large range",
			args:              []string{"export_orderbooks", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "6000000", "-e", "6001000", "-o", gotTestDir(t, "range/")},
			golden:            "large_range_orderbooks.golden",
			sortForComparison: true,
			wantErr:           nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/orderbooks/")
	}
}
