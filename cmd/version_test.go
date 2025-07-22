package cmd

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestVersionCommand(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Execute the version command
	cmd := &cobra.Command{}
	cmd.AddCommand(versionCmd)

	// Set the args to version
	cmd.SetArgs([]string{"version"})

	// Execute command
	err := cmd.Execute()

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Read the output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Verify no error occurred
	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}

	// Verify output contains expected content
	if !strings.Contains(output, "stellar-etl version:") {
		t.Errorf("Expected output to contain 'stellar-etl version:', got: %s", output)
	}

	if !strings.Contains(output, "Stellar XDR version") {
		t.Errorf("Expected output to contain 'Stellar XDR version', got: %s", output)
	}
}
