package cmd

import (
	"os"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
	"github.com/stretchr/testify/assert"
)

// TestProcessLedger_TransformFailureIsFatal verifies the end-to-end wiring:
// when StrictExport is true and transform.TransformLedger returns an error,
// processLedger routes the error through cmdLogger.LogError, which must
// terminate the run via the configured exit func.
func TestProcessLedger_TransformFailureIsFatal(t *testing.T) {
	// Save and restore global cmdLogger state so we don't leak into other tests.
	origStrict := cmdLogger.StrictExport
	t.Cleanup(func() {
		cmdLogger.StrictExport = origStrict
		cmdLogger.SetExitFunc(os.Exit)
	})

	cmdLogger.StrictExport = true
	var exitCode int
	exitCalled := false
	cmdLogger.SetExitFunc(func(code int) {
		exitCalled = true
		exitCode = code
		// Panic to abort execution here, matching the real os.Exit contract
		// (processLedger must not continue past a fatal LogError call).
		panic("exit called")
	})

	// TotalCoins < 0 makes transform.TransformLedger fail with a deterministic
	// error. HistoryArchiveLedgerFromLCM reads the header out of lcm.V1, so
	// that's where the sentinel value goes.
	badLCM := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{TotalCoins: -1},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{}},
			},
			Ext: xdr.LedgerCloseMetaExt{
				V:  1,
				V1: &xdr.LedgerCloseMetaExtV1{},
			},
		},
	}

	outFile, err := os.CreateTemp(t.TempDir(), "processLedger-fatal-*.txt")
	assert.NoError(t, err)
	defer outFile.Close()

	assert.PanicsWithValue(t, "exit called", func() {
		processLedger(badLCM, utils.EnvironmentDetails{}, outFile, false, nil)
	}, "processLedger must be fatal on transform failure under StrictExport")

	assert.True(t, exitCalled, "expected transform failure to invoke logger exit")
	assert.Equal(t, 1, exitCode, "expected exit code 1")
}
