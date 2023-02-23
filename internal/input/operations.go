package input

import (
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// OperationTransformInput is a representation of the input for the TransformOperation function
type OperationTransformInput struct {
	Operation      xdr.Operation
	OperationIndex int32
	Transaction    ingest.LedgerTransaction
	LedgerSeqNum   int32
}

func panicIf(err error) {
	if err != nil {
		panic(fmt.Errorf("An error occurred, panicking: %s\n", err))
	}
}

// GetOperations returns a slice of operations for the ledgers in the provided range (inclusive on both ends)
func GetOperations(start, end uint32, limit int64, isTest bool) ([]OperationTransformInput, error) {
	opSlice := []OperationTransformInput{}
	env := utils.GetEnvironmentDetails(isTest)
	slcm, err := GetLedgers(start, end, -1, isTest)
	if err != nil {
		log.Error("Error creating GCS backend:", err)
		return []OperationTransformInput{}, err
	}
	for seq := uint32(0); seq <= end-start; seq++ {
		txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(env.NetworkPassphrase, *slcm[seq].V0)
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
