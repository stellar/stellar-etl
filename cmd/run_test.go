package cmd

import (
	"fmt"
	"testing"
)

func TestIntegration(t *testing.T) {
	tests := []cliTest{}

	for _, test := range tests {
		fmt.Println(test)
		runCLITest(t, test, "testdata/transactions/")
	}
}
