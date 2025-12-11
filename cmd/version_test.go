package cmd

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionCommand(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.AddCommand(versionCmd)
	out := new(bytes.Buffer)
	cmd.SetOut(out)
	cmd.SetArgs([]string{"version"})
	err := cmd.Execute()
	require.NoError(t, err)

	outStr := out.String()
	assert.Contains(t, outStr, "stellar-etl")
	assert.Contains(t, outStr, "github.com/stellar/go-stellar-sdk")
	assert.Contains(t, outStr, "github.com/stellar/go-stellar-sdk-stellar-xdr-json")
}
