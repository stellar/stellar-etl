package input

import (
	"context"
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/stellar-etl/v2/internal/toid"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// AllHistoryTransformInput is a representation of the input for the TransformOperation function
type AllHistoryTransformInput struct {
	Operations []OperationTransformInput
	Trades     []TradeTransformInput
	Ledgers    []LedgerTransformInput
}

// GetAllHistory returns a slice of operations, trades, effects, transactions, diagnostic events
// for the ledgers in the provided range (inclusive on both ends)
func GetAllHistory(start, end uint32, limit int64, env utils.EnvironmentDetails, useCaptiveCore bool) (AllHistoryTransformInput, error) {
	ctx := context.Background()

	backend, err := utils.CreateLedgerBackend(ctx, useCaptiveCore, env)
	if err != nil {
		return AllHistoryTransformInput{}, err
	}

	opSlice := []OperationTransformInput{}
	tradeSlice := []TradeTransformInput{}
	txSlice := []LedgerTransformInput{}
	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(start, end))
	panicIf(err)
	for seq := start; seq <= end; seq++ {
		changeReader, err := ingest.NewLedgerChangeReader(ctx, backend, env.NetworkPassphrase, seq)
		if err != nil {
			return AllHistoryTransformInput{}, err
		}
		txReader := changeReader.LedgerTransactionReader

		ledgerCloseMeta, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return AllHistoryTransformInput{}, fmt.Errorf("error getting ledger seq %d from the backend: %v", seq, err)
		}

		closeTime, err := utils.TimePointToUTCTimeStamp(txReader.GetHeader().Header.ScpValue.CloseTime)
		if err != nil {
			return AllHistoryTransformInput{}, err
		}

		lhe := txReader.GetHeader()

		for limit < 0 {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}

			for index, op := range tx.Envelope.Operations() {
				// Operations
				opSlice = append(opSlice, OperationTransformInput{
					Operation:       op,
					OperationIndex:  int32(index),
					Transaction:     tx,
					LedgerSeqNum:    int32(seq),
					LedgerCloseMeta: ledgerCloseMeta,
				})

				// Trades
				if operationResultsInTrade(op) && tx.Result.Successful() {
					tradeSlice = append(tradeSlice, TradeTransformInput{
						OperationIndex:     int32(index),
						Transaction:        tx,
						CloseTime:          closeTime,
						OperationHistoryID: toid.New(int32(seq), int32(tx.Index), int32(index)).ToInt64(),
					})
				}
			}
			// Transactions
			txSlice = append(txSlice, LedgerTransformInput{
				Transaction:     tx,
				LedgerHistory:   lhe,
				LedgerCloseMeta: ledgerCloseMeta,
			})

		}

		txReader.Close()
	}

	allHistoryTransformInput := AllHistoryTransformInput{
		Operations: opSlice,
		Trades:     tradeSlice,
		Ledgers:    txSlice,
	}

	return allHistoryTransformInput, nil
}
