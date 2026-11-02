package transform

import (
	"fmt"
	"strconv"

	"github.com/guregu/null"
	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/toid"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

func TransformTokenTransfer(ledgerCloseMeta xdr.LedgerCloseMeta, networkPassphrase string) ([]TokenTransferOutput, error) {
	eventsProcessor := token_transfer.NewEventsProcessorForUnifiedEvents(networkPassphrase)

	events, err := eventsProcessor.EventsFromLedger(ledgerCloseMeta)
	if err != nil {
		return []TokenTransferOutput{}, err
	}

	err = token_transfer.VerifyEvents(ledgerCloseMeta, networkPassphrase, true)
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

	scpValueInfo, err := utils.ScpValueInfoFromLedgerCloseMeta(ledgerCloseMeta)
	if err != nil {
		return []TokenTransferOutput{}, err
	}

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
			muxedAddress, muxedID, err := encodeMuxedDestination(to.String, event.Meta.ToMuxedInfo)
			if err != nil {
				return []TokenTransferOutput{}, err
			}
			toMuxed = null.StringFrom(muxedAddress)
			toMuxedID = null.StringFrom(muxedID)
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
			ClosedAtMs:      scpValueInfo.CloseTimeMs,
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

// encodeMuxedDestination rebuilds the multiplexed destination address that the
// event reported in two separate pieces.
//
// A transfer to a multiplexed destination never travels as one address. The
// Stellar Asset Contract puts the plain underlying address in the event topics
// and the multiplexing id in the data map under to_muxed_id, so the muxed form
// only exists if we reassemble it. That makes this the one place in the repo
// that has to know which kind of address it is holding.
//
// CAP-67 gave accounts an M strkey (G plus a uint64). CAP-84 gives contracts a
// W strkey (C plus a uint64) with an identical 40 byte layout. The builders are
// not interchangeable: strkey.MuxedAccount.SetAccountID rejects a C address, so
// before this branch existed a muxed contract destination produced an empty
// to_muxed and two discarded errors.
func encodeMuxedDestination(to string, muxedInfo *token_transfer.MuxedInfo) (string, string, error) {
	var id uint64
	switch info := muxedInfo.Content.(type) {
	case *token_transfer.MuxedInfo_Id:
		id = info.Id

	case *token_transfer.MuxedInfo_Text, *token_transfer.MuxedInfo_Hash:
		// A memo, not a multiplexing id. MuxedInfo.GetId() reports 0 for these
		// variants, and 0 is what every text and hash memo has been written as
		// since this column existed. That is wrong, but it is deliberately left
		// alone here: changing it would silently restate millions of rows a day
		// as a side effect of a dated protocol release. stellar/hubble#726 owns
		// the fix and the backfill decision.
		id = 0

	default:
		return "", "", fmt.Errorf("unknown muxed info variant %T for destination %q", info, to)
	}

	var address string
	var err error
	if strkey.IsValidContractAddress(to) {
		// CAP-84: a contract destination multiplexes into a W strkey.
		muxedContract := strkey.MuxedContract{}
		if err = muxedContract.SetContractID(to); err == nil {
			muxedContract.SetID(id)
			address, err = muxedContract.Address()
		}
	} else {
		// CAP-67: an account destination multiplexes into an M strkey.
		muxedAccount := strkey.MuxedAccount{}
		if err = muxedAccount.SetAccountID(to); err == nil {
			muxedAccount.SetID(id)
			address, err = muxedAccount.Address()
		}
	}
	if err != nil {
		return "", "", fmt.Errorf("could not encode muxed destination %q: %w", to, err)
	}

	return address, strconv.FormatUint(id, 10), nil
}
