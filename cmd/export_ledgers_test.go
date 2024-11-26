package cmd

import (
	"flag"
	"fmt"
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
	buildCmd := exec.Command("go", "build", "-o", executableName)
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
			Name:    "end before start",
			Args:    []string{"export_ledgers", "-s", "100", "-e", "50"},
			Golden:  "",
			WantErr: fmt.Errorf("Number of bytes written: 0"),
		},
		{
			Name:    "start is 0",
			Args:    []string{"export_ledgers", "-s", "0", "-e", "4294967295", "-l", "4294967295"},
			Golden:  "",
			WantErr: fmt.Errorf("could not read ledgers: LedgerCloseMeta for sequence 0 not found in the batch"),
		},
		{
			Name:    "end is 0",
			Args:    []string{"export_ledgers", "-e", "0", "-l", "4294967295"},
			Golden:  "",
			WantErr: fmt.Errorf("Number of bytes written: 0"),
		},
		{
			Name:    "single ledger",
			Args:    []string{"export_ledgers", "-s", "30822015", "-e", "30822015", "-o", GotTestDir(t, "single_ledger.txt")},
			Golden:  "single_ledger.golden",
			WantErr: nil,
		},
		{
			Name:    "10 ledgers",
			Args:    []string{"export_ledgers", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers.txt")},
			Golden:  "10_ledgers.golden",
			WantErr: nil,
		},
		{
			Name:    "range too large",
			Args:    []string{"export_ledgers", "-s", "30822015", "-e", "30822025", "-l", "5", "-o", GotTestDir(t, "large_range_ledgers.txt")},
			Golden:  "large_range_ledgers.golden",
			WantErr: nil,
		},
		{
			Name:    "range from 2024",
			Args:    []string{"export_ledgers", "-s", "52929555", "-e", "52929960", "-o", GotTestDir(t, "2024_ledgers.txt")},
			Golden:  "2024_ledgers.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/ledgers/")
	}
}
