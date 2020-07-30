package input

import (
	"io"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
)

// OperationTransformInput is a representation of the input for the TransformOperation function
type OperationTransformInput struct {
	Operation      xdr.Operation
	OperationIndex int32
	Transaction    ingestio.LedgerTransaction
}

// GetOperations returns a slice of operation close metas for the ledgers in the provided range (inclusive on both ends)
func GetOperations(start, end uint32, limit int64) ([]OperationTransformInput, error) {
	backend, err := createBackend()
	if err != nil {
		return []OperationTransformInput{}, err
	}

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
			if err == io.EOF {
				break
			}

			for index, op := range tx.Envelope.Operations() {
				opSlice = append(opSlice, OperationTransformInput{
					Operation:      op,
					OperationIndex: int32(index),
					Transaction:    tx,
				})

				if int64(len(opSlice)) >= limit && limit >= 0 {
					break
				}
			}
		}

		if int64(len(opSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return opSlice, nil
}
