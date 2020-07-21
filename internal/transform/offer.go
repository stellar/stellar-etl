package transform

import (
	"fmt"
	"strconv"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
)

//TransformOffer converts an account from the history archive ingestion system into a form suitable for BigQuery
func TransformOffer(ledgerChange ingestio.Change) (OfferOutput, error) {
	outputOfferDeleted := false
	ledgerEntry := ledgerChange.Post
	if ledgerChange.LedgerEntryChangeType() == xdr.LedgerEntryChangeTypeLedgerEntryRemoved {
		outputOfferDeleted = true
		ledgerEntry = ledgerChange.Pre
	}
	offerEntry, offerFound := ledgerEntry.Data.GetOffer()
	if !offerFound {
		return OfferOutput{}, fmt.Errorf("Could not extract offer data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	outputSellerID, err := offerEntry.SellerId.GetAddress()
	if err != nil {
		return OfferOutput{}, err
	}
	outputOfferID := int64(offerEntry.OfferId)

	outputSellingAsset, _ := xdr.MarshalBase64(offerEntry.Selling) //check err
	outputBuyingAsset, _ := xdr.MarshalBase64(offerEntry.Buying)

	outputAmount := int64(offerEntry.Amount) //check neg

	outputPriceN := int32(offerEntry.Price.N) //check neg
	outputPriceD := int32(offerEntry.Price.D) //check neg and non 0

	outputPrice, _ := strconv.ParseFloat(offerEntry.Price.String(), 64) //check neg; err

	outputFlags := uint32(offerEntry.Flags)
	if outputFlags < 0 {
		return OfferOutput{}, fmt.Errorf("Flags are negative (%d)for account: %s", outputFlags, outputSellerID)
	}

	outputLastModifiedLedger := int64(ledgerEntry.LastModifiedLedgerSeq)
	if outputLastModifiedLedger < 0 {
		return OfferOutput{}, fmt.Errorf("Last modified ledger number is negative (%d) for account: %s", outputLastModifiedLedger, outputSellerID)
	}

	//outputDeleted := offerEntry.

	transformedOffer := OfferOutput{
		SellerID:           outputSellerID,
		OfferID:            outputOfferID,
		SellingAsset:       outputSellingAsset,
		BuyingAsset:        outputBuyingAsset,
		Amount:             outputAmount,
		PriceN:             outputPriceN,
		PriceD:             outputPriceD,
		Price:              outputPrice,
		LastModifiedLedger: outputLastModifiedLedger,
		Deleted:            outputOfferDeleted,
	}
	return transformedOffer, nil
}
