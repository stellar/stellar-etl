package input

import (
	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// LedgerTransformInput is a representation of the input for the TransformTransaction function
type LedgerTransformInput struct {
	Transaction   ingestio.LedgerTransaction
	LedgerHistory xdr.LedgerHeaderHistoryEntry
}

var publicPassword = network.PublicNetworkPassphrase

// GetTransactions returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetTransactions(start, end uint32, limit int64) ([]LedgerTransformInput, error) {
	backend, err := utils.CreateBackend()
	if err != nil {
		return []LedgerTransformInput{}, err
	}

	defer backend.Close()

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
		if err != nil {
			return []LedgerTransformInput{}, err
		}

		lhe := txReader.GetHeader()
		// A negative limit value means that all input should be processed
		for int64(len(txSlice)) < limit || limit < 0 {
			tx, err := txReader.Read()
			if err == ingestio.EOF {
				break
			}

			txSlice = append(txSlice, LedgerTransformInput{
				Transaction:   tx,
				LedgerHistory: lhe,
			})
		}

		txReader.Close()
		if int64(len(txSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return txSlice, nil
}
