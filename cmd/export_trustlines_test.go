package cmd

import (
	"testing"
)

func TestExportTrustlines(t *testing.T) {
	tests := []cliTest{
		{
			name:    "trustlines: bucket list with exact checkpoint",
			args:    []string{"export_trustlines", "-e", "78975", "-o", gotTestDir(t, "bucket_read_exact.golden")},
			golden:  "bucket_read_exact.golden",
			wantErr: nil,
		},
		{
			name:    "trustlines: bucket list with end not on checkpoint",
			args:    []string{"export_trustlines", "-e", "139672", "-o", gotTestDir(t, "bucket_read_off.golden")},
			golden:  "bucket_read_off.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/trustlines/")
	}
}
