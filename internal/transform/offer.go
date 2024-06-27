package transform

import (
	"fmt"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TransformOffer converts an account from the history archive ingestion system into a form suitable for BigQuery
func TransformOffer(ledgerChange ingest.Change, header xdr.LedgerHeaderHistoryEntry) (OfferOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return OfferOutput{}, err
	}

	offerEntry, offerFound := ledgerEntry.Data.GetOffer()
	if !offerFound {
		return OfferOutput{}, fmt.Errorf("could not extract offer data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	outputSellerID, err := offerEntry.SellerId.GetAddress()
	if err != nil {
		return OfferOutput{}, err
	}

	outputOfferID := int64(offerEntry.OfferId)
	if outputOfferID < 0 {
		return OfferOutput{}, fmt.Errorf("offerID is negative (%d) for offer from account: %s", outputOfferID, outputSellerID)
	}

	outputSellingAsset, err := transformSingleAsset(offerEntry.Selling)
	if err != nil {
		return OfferOutput{}, err
	}

	outputBuyingAsset, err := transformSingleAsset(offerEntry.Buying)
	if err != nil {
		return OfferOutput{}, err
	}

	outputAmount := offerEntry.Amount
	if outputAmount < 0 {
		return OfferOutput{}, fmt.Errorf("amount is negative (%d) for offer %d", outputAmount, outputOfferID)
	}

	outputPriceN := int32(offerEntry.Price.N)
	if outputPriceN < 0 {
		return OfferOutput{}, fmt.Errorf("price numerator is negative (%d) for offer %d", outputPriceN, outputOfferID)
	}

	outputPriceD := int32(offerEntry.Price.D)
	if outputPriceD == 0 {
		return OfferOutput{}, fmt.Errorf("price denominator is 0 for offer %d", outputOfferID)
	}

	if outputPriceD < 0 {
		return OfferOutput{}, fmt.Errorf("price denominator is negative (%d) for offer %d", outputPriceD, outputOfferID)
	}

	var outputPrice float64
	if outputPriceN > 0 {
		outputPrice = float64(outputPriceN) / float64(outputPriceD)
	}

	outputFlags := uint32(offerEntry.Flags)

	outputLastModifiedLedger := uint32(ledgerEntry.LastModifiedLedgerSeq)

	closedAt, err := utils.TimePointToUTCTimeStamp(header.Header.ScpValue.CloseTime)
	if err != nil {
		return OfferOutput{}, err
	}

	ledgerSequence := header.Header.LedgerSeq

	transformedOffer := OfferOutput{
		SellerID:           outputSellerID,
		OfferID:            outputOfferID,
		SellingAssetType:   outputSellingAsset.AssetType,
		SellingAssetCode:   outputSellingAsset.AssetCode,
		SellingAssetIssuer: outputSellingAsset.AssetIssuer,
		SellingAssetID:     outputSellingAsset.AssetID,
		BuyingAssetType:    outputBuyingAsset.AssetType,
		BuyingAssetCode:    outputBuyingAsset.AssetCode,
		BuyingAssetIssuer:  outputBuyingAsset.AssetIssuer,
		BuyingAssetID:      outputBuyingAsset.AssetID,
		Amount:             utils.ConvertStroopValueToReal(outputAmount),
		PriceN:             outputPriceN,
		PriceD:             outputPriceD,
		Price:              outputPrice,
		Flags:              outputFlags,
		LastModifiedLedger: outputLastModifiedLedger,
		LedgerEntryChange:  uint32(changeType),
		Deleted:            outputDeleted,
		Sponsor:            ledgerEntrySponsorToNullString(ledgerEntry),
		ClosedAt:           closedAt,
		LedgerSequence:     uint32(ledgerSequence),
	}
	return transformedOffer, nil
}
