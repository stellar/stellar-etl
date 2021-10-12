package transform

import (
	"fmt"
	"time"

	"github.com/guregu/null"
	"github.com/pkg/errors"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/toid"
)

// TransformTrade converts a relevant operation from the history archive ingestion system into a form suitable for BigQuery
func TransformTrade(operationIndex int32, operationID int64, transaction ingest.LedgerTransaction, ledgerCloseTime time.Time) ([]TradeOutput, error) {
	operationResults, ok := transaction.Result.OperationResults()
	if !ok {
		return []TradeOutput{}, fmt.Errorf("Could not get any results from this transaction")
	}

	if !transaction.Result.Successful() {
		return []TradeOutput{}, fmt.Errorf("Transaction failed; no trades")
	}

	operation := transaction.Envelope.Operations()[operationIndex]
	// operation id is +1 incremented to stay in sync with ingest package
	outputOperationID := operationID + 1
	claimedOffers, BuyingOffer, err := extractClaimedOffers(operationResults, operationIndex, operation.Body.Type)
	if err != nil {
		return []TradeOutput{}, err
	}

	transformedTrades := []TradeOutput{}

	for claimOrder, claimOffer := range claimedOffers {
		outputOrder := int32(claimOrder)
		outputLedgerClosedAt := ledgerCloseTime

		var outputSellingAssetType, outputSellingAssetCode, outputSellingAssetIssuer string
		err = claimOffer.AssetSold().Extract(&outputSellingAssetType, &outputSellingAssetCode, &outputSellingAssetIssuer)
		if err != nil {
			return []TradeOutput{}, err
		}

		outputSellingAmount := int64(claimOffer.AmountSold())
		if outputSellingAmount < 0 {
			return []TradeOutput{}, fmt.Errorf("Amount sold is negative (%d) for operation at index %d", outputSellingAmount, operationIndex)
		}

		var outputBuyingAssetType, outputBuyingAssetCode, outputBuyingAssetIssuer string
		err = claimOffer.AssetBought().Extract(&outputBuyingAssetType, &outputBuyingAssetCode, &outputBuyingAssetIssuer)
		if err != nil {
			return []TradeOutput{}, err
		}

		outputBuyingAmount := int64(claimOffer.AmountBought())
		if outputBuyingAmount < 0 {
			return []TradeOutput{}, fmt.Errorf("Amount bought is negative (%d) for operation at index %d", outputBuyingAmount, operationIndex)
		}

		if outputSellingAmount == 0 && outputBuyingAmount == 0 {
			return []TradeOutput{}, fmt.Errorf("Both Selling and Buying amount are 0 for operation at index %d", operationIndex)
		}

		// Final price should be buy / sell
		outputPriceN, outputPriceD, err := findTradeSellPrice(transaction, operationIndex, claimOffer)

		var outputSellingAccountAddress string
		var liquidityPoolID null.String
		var outputPoolFee null.Int
		var outputSellingOfferID, outputBuyingOfferID null.Int
		if claimOffer.Type == xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool {
			id := claimOffer.MustLiquidityPool().LiquidityPoolId
			liquidityPoolID = null.StringFrom(PoolIDToString(id))
			var fee uint32
			if fee, err = findPoolFee(transaction, operationIndex, id); err != nil {
				return []TradeOutput{}, fmt.Errorf("Cannot parse fee for liquidity pool %v", liquidityPoolID)
			}
			outputPoolFee = null.IntFrom(int64(fee))
		} else {
			outputSellingOfferID = null.IntFrom(int64(claimOffer.OfferId()))
			outputSellingAccountAddress = claimOffer.SellerId().Address()
		}

		if BuyingOffer != nil {
			outputBuyingOfferID = null.IntFrom(int64(claimOffer.OfferId()))
		} else {
			outputBuyingOfferID = null.IntFrom(toid.EncodeOfferId(uint64(operationID), toid.TOIDType))
		}

		var outputBuyingAccountAddress string
		if buyer := operation.SourceAccount; buyer != nil {
			accid := buyer.ToAccountId()
			outputBuyingAccountAddress = accid.Address()
		} else {
			sa := transaction.Envelope.SourceAccount().ToAccountId()
			outputBuyingAccountAddress = sa.Address()
		}

		trade := TradeOutput{
			Order:                  outputOrder,
			LedgerClosedAt:         outputLedgerClosedAt,
			SellingAccountAddress:  outputSellingAccountAddress,
			SellingAssetType:       outputSellingAssetType,
			SellingAssetCode:       outputSellingAssetCode,
			SellingAssetIssuer:     outputSellingAssetIssuer,
			SellingAmount:          float64(outputSellingAmount) * 0.0000001,
			BuyingAccountAddress:   outputBuyingAccountAddress,
			BuyingAssetType:        outputBuyingAssetType,
			BuyingAssetCode:        outputBuyingAssetCode,
			BuyingAssetIssuer:      outputBuyingAssetIssuer,
			BuyingAmount:           float64(outputBuyingAmount) * 0.0000001,
			PriceN:                 outputPriceN,
			PriceD:                 outputPriceD,
			SellingOfferID:         outputSellingOfferID,
			BuyingOfferID:          outputBuyingOfferID,
			SellingLiquidityPoolID: liquidityPoolID,
			LiquidityPoolFee:       outputPoolFee,
			HistoryOperationID:     outputOperationID,
		}

		transformedTrades = append(transformedTrades, trade)
	}
	return transformedTrades, nil
}

func extractClaimedOffers(operationResults []xdr.OperationResult, operationIndex int32, operationType xdr.OperationType) (claimedOffers []xdr.ClaimAtom, BuyingOffer *xdr.OfferEntry, err error) {
	if operationIndex >= int32(len(operationResults)) {
		err = fmt.Errorf("Operation index of %d is out of bounds in result slice (len = %d)", operationIndex, len(operationResults))
		return
	}

	if operationResults[operationIndex].Tr == nil {
		err = fmt.Errorf("Could not get result Tr for operation at index %d", operationIndex)
		return
	}

	operationTr, ok := operationResults[operationIndex].GetTr()
	if !ok {
		err = fmt.Errorf("Could not get result Tr for operation at index %d", operationIndex)
		return
	}

	switch operationType {
	case xdr.OperationTypeManageBuyOffer:
		var buyOfferResult xdr.ManageBuyOfferResult
		if buyOfferResult, ok = operationTr.GetManageBuyOfferResult(); !ok {
			err = fmt.Errorf("Could not get ManageBuyOfferResult for operation at index %d", operationIndex)
			return
		}

		if success, ok := buyOfferResult.GetSuccess(); ok {
			claimedOffers = success.OffersClaimed
			BuyingOffer = success.Offer.Offer
			return
		}

		err = fmt.Errorf("Could not get ManageOfferSuccess for operation at index %d", operationIndex)

	case xdr.OperationTypeManageSellOffer:
		var sellOfferResult xdr.ManageSellOfferResult
		if sellOfferResult, ok = operationTr.GetManageSellOfferResult(); !ok {
			err = fmt.Errorf("Could not get ManageSellOfferResult for operation at index %d", operationIndex)
			return
		}

		if success, ok := sellOfferResult.GetSuccess(); ok {
			claimedOffers = success.OffersClaimed
			BuyingOffer = success.Offer.Offer
			return
		}

		err = fmt.Errorf("Could not get ManageOfferSuccess for operation at index %d", operationIndex)

	case xdr.OperationTypeCreatePassiveSellOffer:
		// KNOWN ISSUE: stellar-core creates results for CreatePassiveOffer operations
		// with the wrong result arm set.
		if operationTr.Type == xdr.OperationTypeManageSellOffer {
			passiveSellResult := operationTr.MustManageSellOfferResult().MustSuccess()
			claimedOffers = passiveSellResult.OffersClaimed
			BuyingOffer = passiveSellResult.Offer.Offer
			return
		} else {
			passiveSellResult := operationTr.MustCreatePassiveSellOfferResult().MustSuccess()
			claimedOffers = passiveSellResult.OffersClaimed
			BuyingOffer = passiveSellResult.Offer.Offer
			return
		}

	case xdr.OperationTypePathPaymentStrictSend:
		var pathSendResult xdr.PathPaymentStrictSendResult
		if pathSendResult, ok = operationTr.GetPathPaymentStrictSendResult(); !ok {
			err = fmt.Errorf("Could not get PathPaymentStrictSendResult for operation at index %d", operationIndex)
			return
		}

		success, ok := pathSendResult.GetSuccess()
		if ok {
			claimedOffers = success.Offers
			return
		}

		err = fmt.Errorf("Could not get PathPaymentStrictSendSuccess for operation at index %d", operationIndex)

	case xdr.OperationTypePathPaymentStrictReceive:
		var pathReceiveResult xdr.PathPaymentStrictReceiveResult
		if pathReceiveResult, ok = operationTr.GetPathPaymentStrictReceiveResult(); !ok {
			err = fmt.Errorf("Could not get PathPaymentStrictReceiveResult for operation at index %d", operationIndex)
			return
		}

		if success, ok := pathReceiveResult.GetSuccess(); ok {
			claimedOffers = success.Offers
			return
		}

		err = fmt.Errorf("Could not get GetPathPaymentStrictReceiveSuccess for operation at index %d", operationIndex)

	default:
		err = fmt.Errorf("Operation of type %s at index %d does not result in trades", operationType, operationIndex)
		return
	}

	return
}

func findTradeSellPrice(t ingest.LedgerTransaction, operationIndex int32, trade xdr.ClaimAtom) (n, d int64, err error) {
	if trade.Type == xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool {
		return int64(trade.AmountBought()), int64(trade.AmountSold()), nil
	}

	key := xdr.LedgerKey{}
	if err := key.SetOffer(trade.SellerId(), uint64(trade.OfferId())); err != nil {
		return 0, 0, errors.Wrap(err, "Could not create offer ledger key")
	}
	change, err := findLatestOperationChange(t, operationIndex, key)
	if err != nil {
		return 0, 0, errors.Wrap(err, "could not find change for trade offer")
	}

	return int64(change.Pre.Data.MustOffer().Price.N), int64(change.Pre.Data.MustOffer().Price.D), nil
}

func findLatestOperationChange(t ingest.LedgerTransaction, operationIndex int32, key xdr.LedgerKey) (ingest.Change, error) {
	changes, err := t.GetOperationChanges(uint32(operationIndex))
	if err != nil {
		return ingest.Change{}, errors.Wrap(err, "could not determine changes for operation")
	}

	var change ingest.Change
	// traverse through the slice in reverse order
	for i := len(changes) - 1; i >= 0; i-- {
		change = changes[i]
		if change.Pre != nil && key.Equals(change.Pre.LedgerKey()) {
			return change, nil
		}
	}
	return ingest.Change{}, errors.Errorf("could not find operation for key %v", key)
}

func findPoolFee(t ingest.LedgerTransaction, operationIndex int32, poolID xdr.PoolId) (fee uint32, err error) {
	key := xdr.LedgerKey{}
	if err := key.SetLiquidityPool(poolID); err != nil {
		return 0, errors.Wrap(err, "Could not create liquidity pool ledger key")
	}

	change, err := findLatestOperationChange(t, operationIndex, key)
	if err != nil {
		return 0, errors.Wrap(err, "could not find change for liquidity pool")
	}

	return uint32(change.Pre.Data.MustLiquidityPool().Body.MustConstantProduct().Params.Fee), nil
}
