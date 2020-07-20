package transform

import (
	"fmt"

	"github.com/stellar/go/xdr"
)

//TransformAccount converts an account from the history archive ingestion system into a form suitable for BigQuery
func TransformAccount(ledgerEntry xdr.LedgerEntry) (AccountOutput, error) {
	accountEntry, accountFound := ledgerEntry.Data.GetAccount()
	if !accountFound {
		return AccountOutput{}, fmt.Errorf("Could not extract account data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	outputID, err := accountEntry.AccountId.GetAddress()
	if err != nil {
		return AccountOutput{}, err
	}

	outputBalance := int64(accountEntry.Balance)
	if outputBalance < 0 {
		return AccountOutput{}, fmt.Errorf("Balance is negative (%d) for account: %s", outputBalance, outputID)
	}

	accountExtentionInfo, V1Found := accountEntry.Ext.GetV1()
	var outputBuyingLiabilities, outputSellingLiabilities int64
	if V1Found {
		liabilities := accountExtentionInfo.Liabilities
		outputBuyingLiabilities, outputSellingLiabilities = int64(liabilities.Buying), int64(liabilities.Selling)
		if outputBuyingLiabilities < 0 {
			return AccountOutput{}, fmt.Errorf("The buying liabilities count is negative (%d) for account: %s", outputBuyingLiabilities, outputID)
		}

		if outputSellingLiabilities < 0 {
			return AccountOutput{}, fmt.Errorf("The selling liabilities count is negative (%d) for account: %s", outputSellingLiabilities, outputID)
		}
	}

	outputSequenceNumber := int64(accountEntry.SeqNum)
	if outputSequenceNumber < 0 {
		return AccountOutput{}, fmt.Errorf("Account sequence number is negative (%d) for account: %s", outputSequenceNumber, outputID)
	}

	outputNumSubentries := int32(accountEntry.NumSubEntries)
	if outputNumSubentries < 0 {
		return AccountOutput{}, fmt.Errorf("Subentries count is negative (%d) for account: %s", outputNumSubentries, outputID)
	}

	inflationDestAccountID := accountEntry.InflationDest
	var outputInflationDest string
	if inflationDestAccountID != nil {
		outputInflationDest, err = inflationDestAccountID.GetAddress()
		if err != nil {
			return AccountOutput{}, err
		}
	}

	outputFlags := int32(accountEntry.Flags)
	if outputFlags < 0 {
		return AccountOutput{}, fmt.Errorf("Flags are negative (%d)for account: %s", outputFlags, outputID)
	}

	outputHomeDomain := string(accountEntry.HomeDomain)

	outputMasterWeight := int32(accountEntry.MasterKeyWeight())
	outputThreshLow := int32(accountEntry.ThresholdLow())
	outputThreshMed := int32(accountEntry.ThresholdMedium())
	outputThreshHigh := int32(accountEntry.ThresholdHigh())

	outputLastModifiedLedger := int64(ledgerEntry.LastModifiedLedgerSeq)
	if outputSequenceNumber < 0 {
		return AccountOutput{}, fmt.Errorf("Last modified ledger number is negative (%d) for account: %s", outputSequenceNumber, outputID)
	}

	transformedAccount := AccountOutput{
		AccountID:            outputID,
		Balance:              outputBalance,
		BuyingLiabilities:    outputBuyingLiabilities,
		SellingLiabilities:   outputSellingLiabilities,
		SequenceNumber:       outputSequenceNumber,
		NumSubentries:        outputNumSubentries,
		InflationDestination: outputInflationDest,
		Flags:                outputFlags,
		HomeDomain:           outputHomeDomain,
		MasterWeight:         outputMasterWeight,
		ThresholdLow:         outputThreshLow,
		ThresholdMedium:      outputThreshMed,
		ThresholdHigh:        outputThreshHigh,
		LastModifiedLedger:   outputLastModifiedLedger,
	}
	return transformedAccount, nil
}
