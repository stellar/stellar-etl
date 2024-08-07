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
			name:              "All changes from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265350", "-o", gotTestDir(t, "all/")},
			golden:            "all.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "Account changes from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", gotTestDir(t, "accounts/"), "--export-accounts", "true"},
			golden:            "accounts.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "Claimable balance from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", gotTestDir(t, "claimable_balances/"), "--export-balances", "true"},
			golden:            "claimable_balances.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "Trustllines from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", gotTestDir(t, "trustlines/"), "--export-trustlines", "true"},
			golden:            "trustlines.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "Offers from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", gotTestDir(t, "offers/"), "--export-offers", "true"},
			golden:            "offers.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "Pools from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", gotTestDir(t, "pools/"), "--export-pools", "true"},
			golden:            "pools.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "Contract code from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "50666990", "-e", "50666999", "-o", gotTestDir(t, "contract_code/"), "--export-contract-code", "true"},
			golden:            "contract_code.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "Contract data from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "51340657", "-e", "51340757", "-o", gotTestDir(t, "contract_data/"), "--export-contract-data", "true"},
			golden:            "contract_data.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "Config setting from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "50457424", "-e", "50457440", "-o", gotTestDir(t, "config_setting/"), "--export-config-settings", "true"},
			golden:            "config_setting.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
		{
			name:              "ttl from ledger entry",
			args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "50603521", "-e", "50603621", "-o", gotTestDir(t, "ttl/"), "--export-ttl", "true"},
			golden:            "ttl.golden",
			wantErr:           nil,
			sortForComparison: true,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/changes/")
	}
}
