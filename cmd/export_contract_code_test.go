package cmd

import (
	"testing"
)

func TestExportContractCode(t *testing.T) {
	t.Skip("Skipping due to unstable data in Futurenet")
	// TODO: find ledger with data and create testdata
	tests := []cliTest{
		{
			name:    "contract code",
			args:    []string{"export_contract_code", "-e", "78975", "-o", gotTestDir(t, "bucket_read.txt")},
			golden:  "bucket_read.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/contract_code/")
	}
}
