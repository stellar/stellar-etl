package input

import (
	"context"
	"io"
	"time"

	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
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
func GetTrades(start, end uint32, limit int64, env utils.EnvironmentDetails) ([]TradeTransformInput, error) {
	ctx := context.Background()
	backend, err := env.CreateCaptiveCoreBackend()

	tradeSlice := []TradeTransformInput{}
	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(start, end))
	panicIf(err)
	for seq := start; seq <= end; seq++ {
		changeReader, err := ingest.NewLedgerChangeReader(ctx, backend, env.NetworkPassphrase, seq)
		if err != nil {
			return []TradeTransformInput{}, err
		}
		txReader := changeReader.LedgerTransactionReader

		closeTime, err := utils.TimePointToUTCTimeStamp(txReader.GetHeader().Header.ScpValue.CloseTime)
		if err != nil {
			return []TradeTransformInput{}, err
		}

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
