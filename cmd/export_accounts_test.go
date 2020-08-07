package cmd

import (
	"testing"
)

func TestExportAccounts(t *testing.T) {
	tests := []cliTest{
		{
			name:    "bucket list read; exact checkpoint",
			args:    []string{"export_accounts", "-e", "78975", "--stdout"},
			golden:  "bucket_read_exact.golden",
			wantErr: nil,
		},
		{
			name:    "bucket list read; end not on checkpoint",
			args:    []string{"export_accounts", "-e", "80210", "--stdout"},
			golden:  "bucket_read_off.golden",
			wantErr: nil,
		},
		{
			name:    "accounts from one ledger",
			args:    []string{"export_accounts", "-s", "10820015", "-e", "10820015", "--stdout", "-x", "../stellar-core/src/stellar-core", "-c", "../stellar-core/docs/stellar-core_example.cfg"},
			golden:  "one_ledger_accs.golden",
			wantErr: nil,
		},
		{
			name:    "accounts from 10 ledgers",
			args:    []string{"export_accounts", "-s", "17736172", "-e", "17736182", "--stdout", "-x", "../stellar-core/src/stellar-core", "-c", "../stellar-core/docs/stellar-core_example.cfg"},
			golden:  "10_ledgers_accs.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_accounts", "-s", "10820000", "-e", "10820015", "-l", "5", "--stdout", "-x", "../stellar-core/src/stellar-core", "-c", "../stellar-core/docs/stellar-core_example.cfg"},
			golden:  "large_range_accs.golden",
			wantErr: nil,
		},
		{
			name:    "ledger with no accounts",
			args:    []string{"export_accounts", "-s", "2", "-e", "2", "--stdout", "-x", "../stellar-core/src/stellar-core", "-c", "../stellar-core/docs/stellar-core_example.cfg"},
			golden:  "ledger_no_accs.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/accounts/")
	}
}
