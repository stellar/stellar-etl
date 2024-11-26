package cmd

import (
	"fmt"
	"testing"
)

const coreExecutablePath = "../stellar-core/src/stellar-core"
const coreConfigPath = "/etl/docker/stellar-core.cfg"

func TestExportChanges(t *testing.T) {

	tests := []CliTest{
		{
			Name:    "unbounded range with no config",
			Args:    []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-s", "100000"},
			Golden:  "",
			WantErr: fmt.Errorf("stellar-core needs a config file path when exporting ledgers continuously (endNum = 0)"),
		},
		{
			Name:    "0 batch size",
			Args:    []string{"export_ledger_entry_changes", "-b", "0", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "100000", "-e", "164000"},
			Golden:  "",
			WantErr: fmt.Errorf("batch-size (0) must be greater than 0"),
		},
		{
			Name:              "All changes from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265350", "-o", GotTestDir(t, "all/")},
			Golden:            "all.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
		{
			Name:              "Account changes from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "accounts/"), "--export-accounts", "true"},
			Golden:            "accounts.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
		{
			Name:              "Claimable balance from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "claimable_balances/"), "--export-balances", "true"},
			Golden:            "claimable_balances.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
		{
			Name:              "trustlines from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "trustlines/"), "--export-trustlines", "true"},
			Golden:            "trustlines.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
		{
			Name:              "Offers from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "offers/"), "--export-offers", "true"},
			Golden:            "offers.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
		{
			Name:              "Pools from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "pools/"), "--export-pools", "true"},
			Golden:            "pools.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
		{
			Name:              "Contract code from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "50666990", "-e", "50666999", "-o", GotTestDir(t, "contract_code/"), "--export-contract-code", "true"},
			Golden:            "contract_code.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
		{
			Name:              "Contract data from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "51340657", "-e", "51340757", "-o", GotTestDir(t, "contract_data/"), "--export-contract-data", "true"},
			Golden:            "contract_data.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
		{
			Name:              "Config setting from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "50457424", "-e", "50457440", "-o", GotTestDir(t, "config_setting/"), "--export-config-settings", "true"},
			Golden:            "config_setting.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
		{
			Name:              "ttl from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "50603521", "-e", "50603621", "-o", GotTestDir(t, "ttl/"), "--export-ttl", "true"},
			Golden:            "ttl.golden",
			WantErr:           nil,
			sortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/changes/")
	}
}
