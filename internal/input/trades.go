package input

import (
	"context"
	"io"
	"time"

	"github.com/stellar/stellar-etl/v2/internal/toid"
	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// TradeTransformInput is a representation of the input for the TransformTrade function
type TradeTransformInput struct {
	OperationIndex     int32
	Transaction        ingest.LedgerTransaction
	CloseTime          time.Time
	OperationHistoryID int64
}

// GetTrades returns a slice of trades for the ledgers in the provided range (inclusive on both ends)
func GetTrades(start, end uint32, limit int64, env utils.EnvironmentDetails, useCaptiveCore bool) ([]TradeTransformInput, error) {
	ctx := context.Background()

	backend, err := utils.CreateLedgerBackend(ctx, useCaptiveCore, env)
	if err != nil {
		return []TradeTransformInput{}, err
	}

	tradeSlice := []TradeTransformInput{}
	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(start, end))
	panicIf(err)
	for seq := start; seq <= end; seq++ {
		ledgerCloseMeta, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return []TradeTransformInput{}, errors.Wrap(err, "error getting ledger from the backend")
		}

		txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(env.NetworkPassphrase, ledgerCloseMeta)
		if err != nil {
			return []TradeTransformInput{}, err
		}

		closeTime, _ := utils.TimePointToUTCTimeStamp(txReader.GetHeader().Header.ScpValue.CloseTime)

		for int64(len(tradeSlice)) < limit || limit < 0 {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}

			for index, op := range tx.Envelope.Operations() {
				/*
					Trades occur on these operation types:
					manage buy offer, manage sell offer, create passive sell offer, path payment send, and path payment receive
					Not all of these operations will result in trades, but this is checked in TransformTrade (an empty slice is returned if no trades occurred)

					Trades also can only occur when these operations are successful
				*/
				if operationResultsInTrade(op) && tx.Result.Successful() {
					tradeSlice = append(tradeSlice, TradeTransformInput{
						OperationIndex:     int32(index),
						Transaction:        tx,
						CloseTime:          closeTime,
						OperationHistoryID: toid.New(int32(seq), int32(tx.Index), int32(index)).ToInt64(),
					})
				}

				if int64(len(tradeSlice)) >= limit && limit >= 0 {
					break
				}
			}
		}

		txReader.Close()
		if int64(len(tradeSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return tradeSlice, nil
}

// operationResultsInTrade returns true if the operation results in a trade
func operationResultsInTrade(operation xdr.Operation) bool {
	switch operation.Body.Type {
	case xdr.OperationTypeManageBuyOffer:
		return true
	case xdr.OperationTypeManageSellOffer:
		return true
	case xdr.OperationTypeCreatePassiveSellOffer:
		return true
	case xdr.OperationTypePathPaymentStrictReceive:
		return true
	case xdr.OperationTypePathPaymentStrictSend:
		return true
	default:
		return false
	}
}
