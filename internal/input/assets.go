package input

import (
	"context"
	"io"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// GetPaymentOperations returns a slice of payment operations that can include new assets from the ledgers in the provided range (inclusive on both ends)
func GetPaymentOperations(start, end uint32, limit int64) ([]OperationTransformInput, error) {
	backend, err := utils.CreateBackend(start, end)
	if err != nil {
		return []OperationTransformInput{}, err
	}

	opSlice := []OperationTransformInput{}
	ctx := context.Background()
	for seq := start; seq <= end; seq++ {
		txReader, err := ingest.NewLedgerTransactionReader(ctx, backend, publicPassword, seq)
		if err != nil {
			return []OperationTransformInput{}, err
		}

		for int64(len(opSlice)) < limit || limit < 0 {
			tx, err := txReader.Read()
			if err == io.EOF {
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
