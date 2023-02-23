package cmd

import (
	"fmt"
	"testing"
)

// const coreExecutablePath = "../stellar-core/src/stellar-core"
const coreExecutablePath = "/usr/local/bin/stellar-core"

// const coreConfigPath = "./docker/stellar-core.cfg"
const coreConfigPath = "./docker/stellar-core_testnet.cfg"

func TestExportChanges(t *testing.T) {

	tests := []cliTest{
		{
			name:    "unbounded range with no config",
			args:    []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "--testnet", "-s", "1410100"},
			golden:  "",
			wantErr: fmt.Errorf("stellar-core needs a config file path when exporting ledgers continuously (endNum = 0)"),
		},
		{
			name:    "0 batch size",
			args:    []string{"export_ledger_entry_changes", "-b", "0", "-x", coreExecutablePath, "-c", coreConfigPath, "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110"},
			golden:  "",
			wantErr: fmt.Errorf("batch-size (0) must be greater than 0"),
		},
		{
			name:    "changes from single ledger",
			args:    []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410100", "-o", gotTestDir(t, "single/")},
			golden:  "single_ledger.golden",
			wantErr: nil,
		},
		{
			name:    "changes from large range",
			args:    []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410132", "-o", gotTestDir(t, "large_range_changes/")},
			golden:  "large_range_changes.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/changes/")
	}
}
