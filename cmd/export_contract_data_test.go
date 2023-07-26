package cmd

import (
	"testing"
)

func TestExportContractData(t *testing.T) {
	t.Skip("Skipping due to unstable data in Futurenet")
	// TODO: find ledger with data and create testdata
	tests := []cliTest{
		{
			name:    "contract data",
			args:    []string{"export_contract_data", "-e", "78975", "-o", gotTestDir(t, "bucket_read.txt")},
			golden:  "bucket_read.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/contract_data/")
	}
}
