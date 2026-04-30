package input

import (
	"context"
	"io"
	"time"

	"github.com/stellar/stellar-etl/v2/internal/toid"
	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/errors"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TradeTransformInput is a representation of the input for the TransformTrade function
type TradeTransformInput struct {
	OperationIndex     int32
	Transaction        ingest.LedgerTransaction
	CloseTime          time.Time
	OperationHistoryID int64
}

// TradesFromLedger extracts all trade-producing operations from a single
// ledger close meta. Only successful transactions and operations that can
// result in trades are included; TransformTrade still filters out ops that
// happened to produce no trade.
func TradesFromLedger(lcm xdr.LedgerCloseMeta, networkPassphrase string) ([]TradeTransformInput, error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, err
	}
	defer txReader.Close()

	seq := lcm.LedgerSequence()
	closeTime, _ := utils.TimePointToUTCTimeStamp(txReader.GetHeader().Header.ScpValue.CloseTime)

	var trades []TradeTransformInput
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "error reading transaction")
		}
		for index, op := range tx.Envelope.Operations() {
			if operationResultsInTrade(op) && tx.Result.Successful() {
				trades = append(trades, TradeTransformInput{
					OperationIndex:     int32(index),
					Transaction:        tx,
					CloseTime:          closeTime,
					OperationHistoryID: toid.New(int32(seq), int32(tx.Index), int32(index)).ToInt64(),
				})
			}
		}
	}
	return trades, nil
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
