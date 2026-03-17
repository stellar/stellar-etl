package transform

import (
	"fmt"
	"sort"

	"github.com/guregu/null"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// TransformSigners converts account signers from the history archive ingestion system into a form suitable for BigQuery
func TransformSigners(ledgerChange ingest.Change, header xdr.LedgerHeaderHistoryEntry) ([]AccountSignerOutput, error) {
	var signers []AccountSignerOutput

	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return signers, err
	}
	accountEntry, accountFound := ledgerEntry.Data.GetAccount()
	if !accountFound {
		return signers, fmt.Errorf("could not extract signer data from ledger entry of type: %+v", ledgerEntry.Data.Type)
	}

	closedAt, err := utils.TimePointToUTCTimeStamp(header.Header.ScpValue.CloseTime)
	if err != nil {
		return signers, err
	}

	ledgerSequence := header.Header.LedgerSeq
	outputLastModifiedLedger := uint32(ledgerEntry.LastModifiedLedgerSeq)

	sponsors := accountEntry.SponsorPerSigner()
	for signer, weight := range accountEntry.SignerSummary() {
		var sponsor null.String
		if sponsorDesc, isSponsored := sponsors[signer]; isSponsored {
			sponsor = null.StringFrom(sponsorDesc.Address())
		}

		signers = append(signers, AccountSignerOutput{
			AccountID:          accountEntry.AccountId.Address(),
			Signer:             signer,
			Weight:             weight,
			Sponsor:            sponsor,
			LastModifiedLedger: outputLastModifiedLedger,
			LedgerEntryChange:  uint32(changeType),
			Deleted:            outputDeleted,
			ClosedAt:           closedAt,
			LedgerSequence:     uint32(ledgerSequence),
		})
	}

	// For account updates, emit deletion rows for signers present in Pre but absent in Post.
	// Signer deletions don't produce their own ledger entry removal — they're embedded in the
	// account's UPDATED change — so we must diff Pre vs Post to surface them explicitly.
	if changeType == xdr.LedgerEntryChangeTypeLedgerEntryUpdated && ledgerChange.Pre != nil {
		preAccountEntry, preFound := ledgerChange.Pre.Data.GetAccount()
		if !preFound {
			return signers, fmt.Errorf("could not extract pre-state signer data from ledger entry of type: %+v", ledgerChange.Pre.Data.Type)
		}

		postSigners := accountEntry.SignerSummary()
		preSponsors := preAccountEntry.SponsorPerSigner()
		preLastModifiedLedger := uint32(ledgerChange.Pre.LastModifiedLedgerSeq)

		for signer := range preAccountEntry.SignerSummary() {
			if _, stillExists := postSigners[signer]; stillExists {
				continue
			}
			var sponsor null.String
			if sponsorDesc, isSponsored := preSponsors[signer]; isSponsored {
				sponsor = null.StringFrom(sponsorDesc.Address())
			}
			signers = append(signers, AccountSignerOutput{
				AccountID:          preAccountEntry.AccountId.Address(),
				Signer:             signer,
				Weight:             0,
				Sponsor:            sponsor,
				LastModifiedLedger: preLastModifiedLedger,
				LedgerEntryChange:  uint32(xdr.LedgerEntryChangeTypeLedgerEntryRemoved),
				Deleted:            true,
				ClosedAt:           closedAt,
				LedgerSequence:     uint32(ledgerSequence),
			})
		}
	}

	sort.Slice(signers, func(a, b int) bool { return signers[a].Weight < signers[b].Weight })
	return signers, nil
}
