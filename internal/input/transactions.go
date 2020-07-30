package input

import (
	"io"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
)

// LedgerTransformInput is a representation of the input for the TransformTransaction function
type LedgerTransformInput struct {
	Transaction   ingestio.LedgerTransaction
	LedgerHistory xdr.LedgerHeaderHistoryEntry
}

var publicPassword = network.PublicNetworkPassphrase

// GetTransactions returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetTransactions(start, end uint32, limit int64) ([]LedgerTransformInput, error) {
	backend, err := createBackend()
	if err != nil {
		return []LedgerTransformInput{}, err
	}

	latestNum, err := backend.GetLatestLedgerSequence()
	if err != nil {
		return []LedgerTransformInput{}, err
	}

	err = validateLedgerRange(start, end, latestNum)
	if err != nil {
		return []LedgerTransformInput{}, err
	}

	txSlice := []LedgerTransformInput{}
	for seq := start; seq <= end; seq++ {
		txReader, err := ingestio.NewLedgerTransactionReader(backend, publicPassword, seq)
		lhe := txReader.GetHeader()
		if err != nil {
			return []LedgerTransformInput{}, err
		}

		// A negative limit value means that all input should be processed
		for int64(len(txSlice)) < limit || limit < 0 {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}

			txSlice = append(txSlice, LedgerTransformInput{
				Transaction:   tx,
				LedgerHistory: lhe,
			})
		}

		if int64(len(txSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return txSlice, nil
}
