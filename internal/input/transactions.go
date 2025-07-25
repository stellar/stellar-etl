package input

import (
	"context"
	"io"

	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// LedgerTransformInput is a representation of the input for the TransformTransaction function
type LedgerTransformInput struct {
	Transaction     ingest.LedgerTransaction
	LedgerHistory   xdr.LedgerHeaderHistoryEntry
	LedgerCloseMeta xdr.LedgerCloseMeta
}

// GetTransactions returns a slice of transactions for the ledgers in the provided range (inclusive on both ends)
func GetTransactions(start, end uint32, limit int64, env utils.EnvironmentDetails, useCaptiveCore bool) ([]LedgerTransformInput, error) {
	ctx := context.Background()

	backend, err := utils.CreateLedgerBackend(ctx, useCaptiveCore, env)
	if err != nil {
		return []LedgerTransformInput{}, err
	}

	txSlice := []LedgerTransformInput{}
	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(start, end))
	if err != nil {
		return []LedgerTransformInput{}, err
	}
	panicIf(err)
	for seq := start; seq <= end; seq++ {
		ledgerCloseMeta, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return []LedgerTransformInput{}, errors.Wrap(err, "error getting ledger from the backend")
		}

		txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(env.NetworkPassphrase, ledgerCloseMeta)
		if err != nil {
			return []LedgerTransformInput{}, err
		}

		lhe := txReader.GetHeader()

		// A negative limit value means that all input should be processed
		for int64(len(txSlice)) < limit || limit < 0 {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}

			txSlice = append(txSlice, LedgerTransformInput{
				Transaction:     tx,
				LedgerHistory:   lhe,
				LedgerCloseMeta: ledgerCloseMeta,
			})
		}

		txReader.Close()
		if int64(len(txSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return txSlice, nil
}
