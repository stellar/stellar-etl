package cmd

import (
	"testing"
)

func TestExportAccounts(t *testing.T) {
	tests := []cliTest{}

	for _, test := range tests {
		runCLITest(t, test, "testdata/accounts/")
	}
}
