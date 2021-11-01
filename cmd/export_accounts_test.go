package cmd

import (
	"testing"
)

func TestExportAccounts(t *testing.T) {
	tests := []cliTest{
		{
			name:    "accounts: bucket list with exact checkpoint",
			args:    []string{"export_accounts", "-e", "78975", "-o", gotTestDir(t, "bucket_read_exact.txt")},
			golden:  "bucket_read_exact.golden",
			wantErr: nil,
		},
		{
			name:    "accounts: bucket list with end not on checkpoint",
			args:    []string{"export_accounts", "-e", "80210", "-o", gotTestDir(t, "bucket_read_off.txt")},
			golden:  "bucket_read_off.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/accounts/")
	}
}
