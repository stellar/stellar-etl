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
			name:    "end before start",
			args:    []string{"export_ledgers", "-s", "100", "-e", "50"},
			golden:  "",
			wantErr: fmt.Errorf("Number of bytes written: 0"),
		},
		{
			name:    "start is 0",
			args:    []string{"export_ledgers", "-s", "0", "-e", "4294967295", "-l", "4294967295"},
			golden:  "",
			wantErr: fmt.Errorf("could not read ledgers: LedgerCloseMeta for sequence 0 not found in the batch"),
		},
		{
			name:    "end is 0",
			args:    []string{"export_ledgers", "-e", "0", "-l", "4294967295"},
			golden:  "",
			wantErr: fmt.Errorf("Number of bytes written: 0"),
		},
		{
			name:    "single ledger",
			args:    []string{"export_ledgers", "-s", "30822015", "-e", "30822015", "-o", GotTestDir(t, "single_ledger.txt")},
			golden:  "single_ledger.golden",
			wantErr: nil,
		},
		{
			name:    "10 ledgers",
			args:    []string{"export_ledgers", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers.txt")},
			golden:  "10_ledgers.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_ledgers", "-s", "30822015", "-e", "30822025", "-l", "5", "-o", GotTestDir(t, "large_range_ledgers.txt")},
			golden:  "large_range_ledgers.golden",
			wantErr: nil,
		},
		{
			name:    "range from 2024",
			args:    []string{"export_ledgers", "-s", "52929555", "-e", "52929960", "-o", GotTestDir(t, "2024_ledgers.txt")},
			golden:  "2024_ledgers.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/ledgers/")
	}
}
