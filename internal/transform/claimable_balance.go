package transform

import (
	"fmt"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

func transformClaimants(claimants []xdr.Claimant) []Claimant {
	var transformed []Claimant
	for _, c := range claimants {
		cv0 := c.MustV0()
		transformed = append(transformed, Claimant{
			Destination: cv0.Destination.Address(),
			Predicate:   cv0.Predicate,
		})
	}
	return transformed
}

// TransformClaimableBalance converts a claimable balance from the history archive ingestion system into a form suitable for BigQuery
func TransformClaimableBalance(ledgerChange ingest.Change, closedAt time.Time) (ClaimableBalanceOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return ClaimableBalanceOutput{}, err
	}

	balanceEntry, balanceFound := ledgerEntry.Data.GetClaimableBalance()
	if !balanceFound {
		return ClaimableBalanceOutput{}, fmt.Errorf("could not extract claimable balance data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}
	balanceID, err := xdr.MarshalHex(balanceEntry.BalanceId)
	if err != nil {
		return ClaimableBalanceOutput{}, fmt.Errorf("Invalid balanceId in op: %d", uint32(ledgerEntry.LastModifiedLedgerSeq))
	}
	outputFlags := uint32(balanceEntry.Flags())
	outputAsset, err := transformSingleAsset(balanceEntry.Asset)
	if err != nil {
		return ClaimableBalanceOutput{}, err
	}
	outputClaimants := transformClaimants(balanceEntry.Claimants)
	outputAmount := balanceEntry.Amount

	outputLastModifiedLedger := uint32(ledgerEntry.LastModifiedLedgerSeq)

	transformed := ClaimableBalanceOutput{
		BalanceID:          balanceID,
		AssetCode:          outputAsset.AssetCode,
		AssetIssuer:        outputAsset.AssetIssuer,
		AssetType:          outputAsset.AssetType,
		AssetID:            outputAsset.ID,
		Claimants:          outputClaimants,
		AssetAmount:        float64(outputAmount) / 1.0e7,
		Sponsor:            ledgerEntrySponsorToNullString(ledgerEntry),
		LastModifiedLedger: outputLastModifiedLedger,
		LedgerEntryChange:  uint32(changeType),
		Flags:              outputFlags,
		Deleted:            outputDeleted,
		ClosedAt:           closedAt,
	}
	return transformed, nil
}
