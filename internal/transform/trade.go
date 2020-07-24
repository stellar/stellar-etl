package transform

import (
	"fmt"
	"time"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformTrade converts a relevant operation from the history archive ingestion system into a form suitable for BigQuery
func TransformTrade(operationIndex int32, transaction ingestio.LedgerTransaction, ledgerCloseTime time.Time) ([]TradeOutput, error) {
	operationResults, ok := transaction.Result.OperationResults()
	if !ok {
		return []TradeOutput{}, fmt.Errorf("Could not get any results from this transaction")
	}

	if !transaction.Result.Successful() {
		return []TradeOutput{}, fmt.Errorf("Transaction failed; no trades")
	}

	operation := transaction.Envelope.Operations()[operationIndex]
	claimedOffers, err := extractClaimedOffers(operationResults, operationIndex, operation.Body.Type)
	if err != nil {
		return []TradeOutput{}, err
	}

	transformedTrades := []TradeOutput{}
	sourceAccount := transaction.Envelope.SourceAccount()

	for claimOrder, claimOffer := range claimedOffers {
		outputOrder := int32(claimOrder)
		outputLedgerClosedAt := ledgerCloseTime

		outputOfferID := int64(claimOffer.OfferId)
		if outputOfferID < 0 {
			return []TradeOutput{}, fmt.Errorf("Offer ID is negative (%d) for operation at index %d", outputOfferID, operationIndex)
		}

		outputBaseAccountAddress, err := claimOffer.SellerId.GetAddress()
		if err != nil {
			return []TradeOutput{}, err
		}

		var outputBaseAssetType, outputBaseAssetCode, outputBaseAssetIssuer string
		err = claimOffer.AssetSold.Extract(&outputBaseAssetType, &outputBaseAssetCode, &outputBaseAssetIssuer)
		if err != nil {
			return []TradeOutput{}, err
		}

		outputBaseAmount := int64(claimOffer.AmountSold)
		if outputBaseAmount < 0 {
			return []TradeOutput{}, fmt.Errorf("Amount sold is negative (%d) for operation at index %d", outputBaseAmount, operationIndex)
		}

		outputCounterAccountAddress, err := utils.GetAccountAddressFromMuxedAccount(sourceAccount)
		if err != nil {
			return []TradeOutput{}, err
		}

		var outputCounterAssetType, outputCounterAssetCode, outputCounterAssetIssuer string
		err = claimOffer.AssetBought.Extract(&outputCounterAssetType, &outputCounterAssetCode, &outputCounterAssetIssuer)
		if err != nil {
			return []TradeOutput{}, err
		}

		outputCounterAmount := int64(claimOffer.AmountBought)
		if outputCounterAmount < 0 {
			return []TradeOutput{}, fmt.Errorf("Amount bought is negative (%d) for operation at index %d", outputCounterAmount, operationIndex)
		}

		if outputBaseAmount == 0 && outputCounterAmount == 0 {
			return []TradeOutput{}, fmt.Errorf("Both base and counter amount are 0 for operation at index %d", operationIndex)
		}

		// Final price should be buy / sell
		outputPriceN, outputPriceD := outputCounterAmount, outputBaseAmount

		if err != nil {
			return []TradeOutput{}, err
		}

		outputBaseIsSeller := true

		/*
			todo:
			1. figure out how to get base and counter offer id
				base id is the same as the offerid
				counter id:
					if entry.BuyOfferExists {
							buyOfferID = EncodeOfferId(uint64(entry.BuyOfferID), CoreOfferIDType)
						} else {
							buyOfferID = EncodeOfferId(uint64(entry.HistoryOperationID), TOIDType)
					}
		*/

		trade := TradeOutput{
			Order:                 outputOrder,
			LedgerClosedAt:        outputLedgerClosedAt,
			OfferID:               outputOfferID,
			BaseAccountAddress:    outputBaseAccountAddress,
			BaseAssetType:         outputBaseAssetType,
			BaseAssetCode:         outputBaseAssetCode,
			BaseAssetIssuer:       outputBaseAssetIssuer,
			BaseAmount:            outputBaseAmount,
			CounterAccountAddress: outputCounterAccountAddress,
			CounterAssetType:      outputCounterAssetType,
			CounterAssetCode:      outputCounterAssetCode,
			CounterAssetIssuer:    outputCounterAssetIssuer,
			CounterAmount:         outputCounterAmount,
			BaseIsSeller:          outputBaseIsSeller,
			PriceN:                outputPriceN,
			PriceD:                outputPriceD,
		}

		transformedTrades = append(transformedTrades, trade)
	}
	return transformedTrades, nil
}

func extractPrice(op xdr.Operation, operationIndex int32) (n int64, d int64, err error) {
	operationType := op.Body.Type
	var price xdr.Price
	switch operationType {
	case xdr.OperationTypeManageBuyOffer:
		opBody, ok := op.Body.GetManageBuyOfferOp()
		if !ok {
			err = fmt.Errorf("Could not get %s body for operation at index %d", operationType, operationIndex)
			return
		}

		price = opBody.Price
	case xdr.OperationTypeManageSellOffer:
		opBody, ok := op.Body.GetManageSellOfferOp()
		if !ok {
			err = fmt.Errorf("Could not get %s body for operation at index %d", operationType, operationIndex)
			return
		}
		price = opBody.Price
	case xdr.OperationTypeCreatePassiveSellOffer:
		opBody, ok := op.Body.GetCreatePassiveSellOfferOp()
		if !ok {
			err = fmt.Errorf("Could not get %s body for operation at index %d", operationType, operationIndex)
			return
		}

		price = opBody.Price
	default:
		err = fmt.Errorf("Operation of type %s at index %d does not have a price", operationType, operationIndex)
		return
	}

	// Invert the price because offers internally stored as buy offers while in a trade we have reversed the order.
	// The base account is the seller, so prices need to reflect sell price instead of buy price
	price.Invert()
	n, d = int64(price.N), int64(price.D)
	if n < 0 {
		err = fmt.Errorf("Price numerator is non-positive (%d) for operation at index %d", n, operationIndex)
	}

	if d <= 0 {
		err = fmt.Errorf("Price denominator is non-positive (%d) for operation at index %d", d, operationIndex)
	}

	return
}

func extractClaimedOffers(operationResults []xdr.OperationResult, operationIndex int32, operationType xdr.OperationType) (claimedOffers []xdr.ClaimOfferAtom, err error) {
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
		offerResult, ok := operationTr.GetManageBuyOfferResult()
		if !ok {
			err = fmt.Errorf("Could not get %sResult for operation at index %d", operationType, operationIndex)
			return
		}

		success, ok := offerResult.GetSuccess()
		if !ok {
			err = fmt.Errorf("Could not get %sSuccess for operation at index %d", operationType, operationIndex)
			return
		}
		claimedOffers = success.OffersClaimed
	case xdr.OperationTypeManageSellOffer:
		offerResult, ok := operationTr.GetManageSellOfferResult()
		if !ok {
			err = fmt.Errorf("Could not get %sResult for operation at index %d", operationType, operationIndex)
			return

		}

		success, ok := offerResult.GetSuccess()
		if !ok {
			err = fmt.Errorf("Could not get %sSuccess for operation at index %d", operationType, operationIndex)
			return

		}

		claimedOffers = success.OffersClaimed
	case xdr.OperationTypeCreatePassiveSellOffer:
		offerResult, ok := operationTr.GetCreatePassiveSellOfferResult()
		if !ok {
			err = fmt.Errorf("Could not get %sResult for operation at index %d", operationType, operationIndex)
			return

		}

		success, ok := offerResult.GetSuccess()
		if !ok {
			err = fmt.Errorf("Could not get %sSuccess for operation at index %d", operationType, operationIndex)
			return

		}

		claimedOffers = success.OffersClaimed
	case xdr.OperationTypePathPaymentStrictSend:
		offerResult, ok := operationTr.GetPathPaymentStrictSendResult()
		if !ok {
			err = fmt.Errorf("Could not get %sResult for operation at index %d", operationType, operationIndex)
			return

		}

		success, ok := offerResult.GetSuccess()
		if !ok {
			err = fmt.Errorf("Could not get %sSuccess for operation at index %d", operationType, operationIndex)
			return

		}

		claimedOffers = success.Offers
	case xdr.OperationTypePathPaymentStrictReceive:
		offerResult, ok := operationTr.GetPathPaymentStrictReceiveResult()
		if !ok {
			err = fmt.Errorf("Could not get %sResult for operation at index %d", operationType, operationIndex)
			return

		}

		success, ok := offerResult.GetSuccess()
		if !ok {
			err = fmt.Errorf("Could not get %sSuccess for operation at index %d", operationType, operationIndex)
			return
		}

		claimedOffers = success.Offers
	default:
		err = fmt.Errorf("Operation of type %s at index %d does not result in trades", operationType, operationIndex)
		return
	}
	return
}
