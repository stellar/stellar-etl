package input

import (
	"context"
	"io"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// OperationTransformInput is a representation of the input for the TransformOperation function
type OperationTransformInput struct {
	Operation      xdr.Operation
	OperationIndex int32
	Transaction    ingest.LedgerTransaction
	LedgerSeqNum   int32
}

// GetOperations returns a slice of operations for the ledgers in the provided range (inclusive on both ends)
func GetOperations(start, end uint32, limit int64, isTest bool) ([]OperationTransformInput, error) {
	env := utils.GetEnvironmentDetails(isTest)
	backend, err := utils.CreateBackend(start, end, env.ArchiveURLs)
	if err != nil {
		return []OperationTransformInput{}, err
	}

	opSlice := []OperationTransformInput{}
	ctx := context.Background()
	for seq := start; seq <= end; seq++ {
		txReader, err := ingest.NewLedgerTransactionReader(ctx, backend, env.NetworkPassphrase, seq)
		if err != nil {
			return []OperationTransformInput{}, err
		}

		for int64(len(opSlice)) < limit || limit < 0 {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}

			for index, op := range tx.Envelope.Operations() {
				opSlice = append(opSlice, OperationTransformInput{
					Operation:      op,
					OperationIndex: int32(index),
					Transaction:    tx,
					LedgerSeqNum:   int32(seq),
				})

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
