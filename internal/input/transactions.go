package input

import (
	"io"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

// LedgerTransformInput is a representation of the input for the TransformTransaction function
type LedgerTransformInput struct {
	Transaction   ingest.LedgerTransaction
	LedgerHistory xdr.LedgerHeaderHistoryEntry
}

// GetTransactions returns a slice of transactions for the ledgers in the provided range (inclusive on both ends)
func GetTransactions(start, end uint32, limit int64, isTest bool) ([]LedgerTransformInput, error) {
	txSlice := []LedgerTransformInput{}
	env := utils.GetEnvironmentDetails(isTest)
	slcm, err := GetLedgers(start, end, -1, isTest)
	if err != nil {
		log.Error("Error creating GCS backend:", err)
		return []LedgerTransformInput{}, err
	}
	for seq := uint32(0); seq <= end-start; seq++ {
		txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(env.NetworkPassphrase, *slcm[seq].V0)
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
