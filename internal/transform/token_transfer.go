package transform

import (
	"fmt"
	"strconv"

	"github.com/guregu/null"
	"github.com/stellar/go/processors/token_transfer"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/toid"
)

func TransformTokenTransfer(ledgerCloseMeta xdr.LedgerCloseMeta, networkPassphrase string) ([]TokenTransferOutput, error) {
	eventsProcessor := token_transfer.NewEventsProcessor(networkPassphrase)

	events, err := eventsProcessor.EventsFromLedger(ledgerCloseMeta)
	if err != nil {
		return []TokenTransferOutput{}, err
	}

	err = token_transfer.VerifyEvents(ledgerCloseMeta, networkPassphrase, false)
	if err != nil {
		return []TokenTransferOutput{}, err
	}

	var transformedTTP []TokenTransferOutput

	transformedTTP, err = transformEvents(events, ledgerCloseMeta)
	if err != nil {
		return []TokenTransferOutput{}, err
	}

	return transformedTTP, nil
}

func transformEvents(events []*token_transfer.TokenTransferEvent, ledgerCloseMeta xdr.LedgerCloseMeta) ([]TokenTransferOutput, error) {
	var transformedTTP []TokenTransferOutput

	for _, event := range events {
		var assetType, asset string
		var assetCode, assetIssuer null.String
		var from, to null.String
		var amount string
		var amountFloat float64

		switch evt := event.Event.(type) {
		case *token_transfer.TokenTransferEvent_Transfer:
			from = null.StringFrom(evt.Transfer.From)
			to = null.StringFrom(evt.Transfer.To)
			amount = evt.Transfer.Amount
			amountFloat, _ = strconv.ParseFloat(amount, 64)
			amountFloat = amountFloat * 0.0000001
		case *token_transfer.TokenTransferEvent_Mint:
			to = null.StringFrom(evt.Mint.To)
			amount = evt.Mint.Amount
			amountFloat, _ = strconv.ParseFloat(amount, 64)
			amountFloat = amountFloat * 0.0000001
		case *token_transfer.TokenTransferEvent_Burn:
			from = null.StringFrom(evt.Burn.From)
			amount = evt.Burn.Amount
			amountFloat, _ = strconv.ParseFloat(amount, 64)
			amountFloat = amountFloat * 0.0000001
		case *token_transfer.TokenTransferEvent_Clawback:
			from = null.StringFrom(evt.Clawback.From)
			amount = evt.Clawback.Amount
			amountFloat, _ = strconv.ParseFloat(amount, 64)
			amountFloat = amountFloat * 0.0000001
		case *token_transfer.TokenTransferEvent_Fee:
			from = null.StringFrom(evt.Fee.From)
			amount = evt.Fee.Amount
			amountFloat, _ = strconv.ParseFloat(amount, 64)
			amountFloat = amountFloat * 0.0000001
		default:
			return []TokenTransferOutput{}, fmt.Errorf("unknown event type in ledger sequence: %d", event.Meta.LedgerSequence)
		}

		var opID int64
		var opIndex int32
		var operationID null.Int

		eventMeta := event.GetMeta()
		ledgerSequence := eventMeta.LedgerSequence
		transactionIndex := eventMeta.TransactionIndex
		transactionID := toid.New(int32(ledgerSequence), int32(transactionIndex), 0).ToInt64()
		operationIndex := eventMeta.OperationIndex
		if operationIndex != nil {
			opIndex = int32(*operationIndex)
			opID = toid.New(int32(ledgerSequence), int32(transactionIndex), opIndex).ToInt64()
			operationID = null.IntFrom(opID)
		}

		asset, assetType, assetCode, assetIssuer = getAssetFromEvent(event)

		var toMuxedID null.String
		var toMuxed null.String

		if event.Meta.ToMuxedInfo != nil {
			muxedID := event.Meta.ToMuxedInfo.GetId()
			muxedAccount := strkey.MuxedAccount{}
			muxedAccount.SetAccountID(to.String)
			muxedAccount.SetID(muxedID)
			muxedAccountString, _ := muxedAccount.Address()
			toMuxed = null.StringFrom(muxedAccountString)
			toMuxedID = null.StringFrom(strconv.FormatUint(muxedID, 10))
		}

		transformedTTP = append(transformedTTP, TokenTransferOutput{
			TransactionHash: eventMeta.TxHash,
			TransactionID:   transactionID,
			OperationID:     operationID,
			EventTopic:      event.GetEventType(),
			From:            from,
			To:              to,
			Asset:           asset,
			AssetType:       assetType,
			AssetCode:       assetCode,
			AssetIssuer:     assetIssuer,
			AmountRaw:       amount,
			Amount:          amountFloat,
			ContractID:      eventMeta.ContractAddress,
			LedgerSequence:  ledgerSequence,
			ClosedAt:        ledgerCloseMeta.ClosedAt(),
			ToMuxed:         toMuxed,
			ToMuxedID:       toMuxedID,
		})
	}

	return transformedTTP, nil
}

func getAssetFromEvent(event *token_transfer.TokenTransferEvent) (assetConcat, assetType string, assetCode, assetIssuer null.String) {
	if event.GetAsset().GetNative() {
		assetType = "native"
		assetConcat = "native"
	}
	asset := event.GetAsset().GetIssuedAsset()
	if asset != nil {
		if len(asset.AssetCode) > 4 {
			assetType = "credit_alphanum12"
		} else {
			assetType = "credit_alphanum4"
		}

		assetCode = null.StringFrom(asset.AssetCode)
		assetIssuer = null.StringFrom(asset.Issuer)
		assetConcat = fmt.Sprintf("%s:%s:%s", assetType, assetCode.String, assetIssuer.String)
	}

	return assetConcat, assetType, assetCode, assetIssuer
}
