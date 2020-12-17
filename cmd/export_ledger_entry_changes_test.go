package cmd

import (
	"fmt"
	"testing"
)

const coreExecutablePath = "../stellar-core/src/stellar-core"
const coreConfigPath = "./docker/stellar-core.cfg"

func TestExportChanges(t *testing.T) {

	tests := []cliTest{
		{
			name:    "unbounded range with no config",
			args:    []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "--stdout", "-s", "100000", "--stdout"},
			golden:  "",
			wantErr: fmt.Errorf("stellar-core needs a config file path when exporting ledgers continuously (endNum = 0)"),
		},
		{
			name:    "0 batch size",
			args:    []string{"export_ledger_entry_changes", "-b", "0", "-x", coreExecutablePath, "-c", coreConfigPath, "--stdout", "-s", "100000", "-e", "164000", "--stdout"},
			golden:  "",
			wantErr: fmt.Errorf("batch-size (0) must be greater than 0"),
		},
		{
			name:    "changes from single ledger",
			args:    []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "140116", "-e", "140116", "--stdout"},
			golden:  "single_ledger.golden",
			wantErr: nil,
		},
		{
			name:    "changes from large range",
			args:    []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "--stdout", "-s", "100000", "-e", "164000", "--stdout"},
			golden:  "large_range_changes.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/changes/")
	}
}
