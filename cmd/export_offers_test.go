package cmd

import (
	"testing"
)

func TestExportOffers(t *testing.T) {
	tests := []cliTest{
		{
			name:    "offers: bucket list with exact checkpoint",
			args:    []string{"export_offers", "-e", "78975", "-o", gotTestDir(t, "bucket_read_exact.txt")},
			golden:  "bucket_read_exact.golden",
			wantErr: nil,
		},
		{
			name:    "offers: bucket list with end not on checkpoint",
			args:    []string{"export_offers", "-e", "80210", "-o", gotTestDir(t, "bucket_read_offset.txt")},
			golden:  "bucket_read_offset.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/offers/")
	}
}
