package input

import (
	ingestio "github.com/stellar/go/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// GetPaymentOperations returns a slice of payment operations that can include new assets from the ledgers in the provided range (inclusive on both ends)
func GetPaymentOperations(start, end uint32, limit int64) ([]OperationTransformInput, error) {
	backend, err := utils.CreateBackend()
	if err != nil {
		return []OperationTransformInput{}, err
	}

	defer backend.Close()

	latestNum, err := backend.GetLatestLedgerSequence()
	if err != nil {
		return []OperationTransformInput{}, err
	}

	err = validateLedgerRange(start, end, latestNum)
	if err != nil {
		return []OperationTransformInput{}, err
	}

	opSlice := []OperationTransformInput{}
	for seq := start; seq <= end; seq++ {
		txReader, err := ingestio.NewLedgerTransactionReader(backend, publicPassword, seq)
		if err != nil {
			return []OperationTransformInput{}, err
		}

		for int64(len(opSlice)) < limit || limit < 0 {
			tx, err := txReader.Read()
			if err == ingestio.EOF {
				break
			}

			for index, op := range tx.Envelope.Operations() {
				if op.Body.Type == xdr.OperationTypePayment {
					opSlice = append(opSlice, OperationTransformInput{
						Operation:      op,
						OperationIndex: int32(index),
						Transaction:    tx,
						LedgerSeqNum:   int32(seq),
					})
				}

				if int64(len(opSlice)) >= limit && limit >= 0 {
					break
				}
			}
		}

		txReader.Close()
		if int64(len(opSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return opSlice, nil
}
