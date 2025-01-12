package cmd

import (
	"testing"
)

func TestExportExternalData(t *testing.T) {
	tests := []cliTest{
		{
			name:    "external data from retool",
			args:    []string{"export_external_data", "--provider", "retool", "--start-time", "", "--end-time", "", "-o", gotTestDir(t, "external_data_retool.txt"), "--testnet"},
			golden:  "external_data_retool.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/external_data/")
	}
}
