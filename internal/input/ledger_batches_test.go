package input

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
	"github.com/stretchr/testify/assert"
)

// erroringBackend is a minimal LedgerBackend that always fails GetLedger, so we
// can exercise the fatal branch in StreamLedgerBatches without a real backend.
type erroringBackend struct {
	err error
}

func (b *erroringBackend) GetLatestLedgerSequence(context.Context) (uint32, error) {
	return 0, nil
}
func (b *erroringBackend) GetLedger(context.Context, uint32) (xdr.LedgerCloseMeta, error) {
	return xdr.LedgerCloseMeta{}, b.err
}
func (b *erroringBackend) PrepareRange(context.Context, ledgerbackend.Range) error { return nil }
func (b *erroringBackend) IsPrepared(context.Context, ledgerbackend.Range) (bool, error) {
	return true, nil
}
func (b *erroringBackend) Close() error { return nil }

func TestStreamLedgerBatches_FailedMetadataReadIsFatal(t *testing.T) {
	logger := utils.NewEtlLogger()
	var exitCode int
	exitCalled := false
	logger.SetExitFunc(func(code int) {
		exitCalled = true
		exitCode = code
		// Panic so StreamLedgerBatches unwinds here instead of continuing past
		// the Fatalf call (mirrors the real os.Exit semantics for the test).
		panic("exit called")
	})

	var backend ledgerbackend.LedgerBackend = &erroringBackend{err: errors.New("datastore unreachable")}
	batchChan := make(chan LedgerBatch, 1)

	assert.PanicsWithValue(t, "exit called", func() {
		StreamLedgerBatches(&backend, 100, 105, 3, batchChan, logger)
	}, "StreamLedgerBatches should call logger.Fatalf when GetLedger fails")

	assert.True(t, exitCalled, "expected logger exit to be called on backend read failure")
	assert.Equal(t, 1, exitCode, "expected exit code 1")
}
