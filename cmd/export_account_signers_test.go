package cmd

import (
"testing"
)

func TestExportSigners(t *testing.T) {
	tests := []cliTest{
		{
			name:    "signers: bucket list with exact checkpoint",
			args:    []string{"export_signers", "-e", "78975", "-o", gotTestDir(t, "bucket_read_exact.txt")},
			golden:  "bucket_read_exact.golden",
			wantErr: nil,
			sortForComparison: true,
		},
		{
			name:    "signers: bucket list with end not on checkpoint",
			args:    []string{"export_signers", "-e", "80210", "-o", gotTestDir(t, "bucket_read_off.txt")},
			golden:  "bucket_read_off.golden",
			wantErr: nil,
			sortForComparison: true,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/signers/")
	}
}

