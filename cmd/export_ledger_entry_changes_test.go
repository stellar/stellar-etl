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
			SortForComparison: true,
		},
		{
			Name:              "Account changes from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "accounts/"), "--export-accounts", "true"},
			Golden:            "accounts.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "Claimable balance from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "claimable_balances/"), "--export-balances", "true"},
			Golden:            "claimable_balances.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "trustlines from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "trustlines/"), "--export-trustlines", "true"},
			Golden:            "trustlines.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "Offers from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "offers/"), "--export-offers", "true"},
			Golden:            "offers.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "Pools from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "pools/"), "--export-pools", "true"},
			Golden:            "pools.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "Contract code from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "50666990", "-e", "50666999", "-o", GotTestDir(t, "contract_code/"), "--export-contract-code", "true"},
			Golden:            "contract_code.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "Contract data from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "51340657", "-e", "51340757", "-o", GotTestDir(t, "contract_data/"), "--export-contract-data", "true"},
			Golden:            "contract_data.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "Config setting from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "50457424", "-e", "50457440", "-o", GotTestDir(t, "config_setting/"), "--export-config-settings", "true"},
			Golden:            "config_setting.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "ttl from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "50603521", "-e", "50603621", "-o", GotTestDir(t, "ttl/"), "--export-ttl", "true"},
			Golden:            "ttl.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "restored keys from ledger entry",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "58764192", "-e", "58764193", "-o", GotTestDir(t, "restored_key/"), "--export-restored-keys", "true"},
			Golden:            "restored_key.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "No flags exports all types",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265350", "-o", GotTestDir(t, "no_flags/")},
			Golden:            "all.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "Multiple flags - accounts and trustlines",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "accounts_trustlines/"), "--export-accounts", "true", "--export-trustlines", "true"},
			Golden:            "accounts_trustlines.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "Multiple flags - accounts, offers, and pools",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "49265302", "-e", "49265400", "-o", GotTestDir(t, "accounts_offers_pools/"), "--export-accounts", "true", "--export-offers", "true", "--export-pools", "true"},
			Golden:            "accounts_offers_pools.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "All contract-related flags",
			Args:              []string{"export_ledger_entry_changes", "-x", coreExecutablePath, "-c", coreConfigPath, "-s", "51340657", "-e", "51340757", "-o", GotTestDir(t, "all_contract_types/"), "--export-contract-code", "true", "--export-contract-data", "true"},
			Golden:            "all_contract_types.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/changes/", "", false)
	}
}
