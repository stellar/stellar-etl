package cmd

import (
	"flag"
	"os"
	"os/exec"
	"testing"
)

func TestMain(m *testing.M) {
	if err := os.Chdir(".."); err != nil {
		cmdLogger.Error("could not change directory", err)
		os.Exit(1)
	}

	// This does the setup for further tests. It generates an executeable that can be run on the command line by other tests
	buildCmd := exec.Command("go", "build", "-o", "stellar-etl")
	if err := buildCmd.Run(); err != nil {
		cmdLogger.Error("could not build executable", err)
		os.Exit(1)
	}

	flag.Parse()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestExportLedger(t *testing.T) {
	tests := []CliTest{
		{
			Name:              "single ledger",
			Args:              []string{"export_ledgers", "-s", "30822015", "-e", "30822015", "-o", GotTestDir(t, "single_ledger/")},
			Golden:            "single_ledger.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "10 ledgers",
			Args:              []string{"export_ledgers", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers/")},
			Golden:            "10_ledgers.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "10 ledgers across multiple batches",
			Args:              []string{"export_ledgers", "-s", "30822015", "-e", "30822025", "-b", "3", "-o", GotTestDir(t, "multi_batch_ledgers/")},
			Golden:            "10_ledgers.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "range from 2024",
			Args:              []string{"export_ledgers", "-s", "52929555", "-e", "52929960", "-o", GotTestDir(t, "2024_ledgers/")},
			Golden:            "2024_ledgers.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/ledgers/", "", false)
	}
}
