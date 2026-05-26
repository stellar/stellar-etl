package cmd

import "testing"

func TestExportFilenameUsesJSONExtension(t *testing.T) {
	got := exportFilename(100, 201, "transactions")
	want := "100-200-transactions.json"

	if got != want {
		t.Fatalf("exportFilename() = %q, want %q", got, want)
	}
}
