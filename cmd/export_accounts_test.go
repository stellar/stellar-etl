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
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/accounts/")
	}
}
