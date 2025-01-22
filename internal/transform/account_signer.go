package transform

import (
	"fmt"
	"sort"

	"github.com/guregu/null"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledger"
	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformSigners converts account signers from the history archive ingestion system into a form suitable for BigQuery
func TransformSigners(ledgerChange ingest.Change) ([]AccountSignerOutput, error) {
	var signers []AccountSignerOutput

	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return signers, err
	}
	outputLastModifiedLedger := uint32(ledgerEntry.LastModifiedLedgerSeq)
	accountEntry, accountFound := ledgerEntry.Data.GetAccount()
	if !accountFound {
		return signers, fmt.Errorf("could not extract signer data from ledger entry of type: %+v", ledgerEntry.Data.Type)
	}

	closedAt := ledger.ClosedAt(*ledgerChange.Ledger)
	ledgerSequence := ledger.Sequence(*ledgerChange.Ledger)
	outputTransactionID := toid.New(int32(ledgerSequence), int32(ledgerChange.Transaction.Index), 0).ToInt64()
	outputOperationID := toid.New(int32(ledgerSequence), int32(ledgerChange.Transaction.Index), int32(ledgerChange.OperationIndex+1)).ToInt64()

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
			LedgerSequence:     ledgerSequence,
			TransactionID:      outputTransactionID,
			OperationID:        outputOperationID,
		})
	}
	sort.Slice(signers, func(a, b int) bool { return signers[a].Weight < signers[b].Weight })
	return signers, nil
}
