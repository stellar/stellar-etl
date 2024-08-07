package cmd

import (
	"fmt"
	"testing"
)

const coreExecutablePath = "../stellar-core/src/stellar-core"
const coreConfigPath = "/etl/docker/stellar-core.cfg"

func TestExportChanges(t *testing.T) {

	tests := []cliTest{
		{
			name:    "unbounded range with no config",
			args:    []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-s", "100000"},
			golden:  "",
			wantErr: fmt.Errorf("stellar-core needs a config file path when exporting ledgers continuously (endNum = 0)"),
		},
		{
			name:    "0 batch size",
			args:    []string{"export_ledger_entry_changes", "-b", "0", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "100000", "-e", "164000"},
			golden:  "",
			wantErr: fmt.Errorf("batch-size (0) must be greater than 0"),
		},
		{
			name:              "all changes from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265350", "-o", gotTestDir(t, "all/")},
			golden:            "all.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "account changes from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", gotTestDir(t, "accounts/"), "--export-accounts", "true"},
			golden:            "accounts.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/changes/")
	}
}
