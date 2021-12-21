package cmd

import (
	"testing"
)

func TestExportClaimableBalances(t *testing.T) {
	tests := []cliTest{
		{
			name:    "claimable balances",
			args:    []string{"export_claimable_balances", "-e", "32878607", "-o", gotTestDir(t, "bucket_read.txt")},
			golden:  "bucket_read.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/claimable_balances/")
	}
}
