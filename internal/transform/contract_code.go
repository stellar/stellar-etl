package transform

import (
	"encoding/base64"
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformContractCode converts a contract code ledger change entry into a form suitable for BigQuery
func TransformContractCode(ledgerChange ingest.Change) (ContractCodeOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return ContractCodeOutput{}, err
	}

	// LedgerEntryChange must contain a contract code change to be parsed, otherwise skip
	if ledgerEntry.Data.Type != xdr.LedgerEntryTypeContractCode {
		return ContractCodeOutput{}, nil
	}

	contractCode, ok := ledgerEntry.Data.GetContractCode()
	if !ok {
		return ContractCodeOutput{}, fmt.Errorf("Could not extract contract code from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	contractCodeExtV := contractCode.Ext.V

	hashBinary, err := contractCode.Hash.MarshalBinary()
	if err != nil {
		return ContractCodeOutput{}, fmt.Errorf("Could not extract Val from contractData")
	}
	contractCodeHash := base64.StdEncoding.EncodeToString(hashBinary)

	binaryCode, err := contractCode.Body.MarshalBinary()
	if err != nil {
		return ContractCodeOutput{}, fmt.Errorf("Could not extract Val from contractData")
	}
	contractCodeCode := base64.StdEncoding.EncodeToString(binaryCode)

	contractCodeExpirationLedgerSeq := contractCode.ExpirationLedgerSeq

	transformedPool := ContractCodeOutput{
		ContractCodeHash:                contractCodeHash,
		ContractCodeExtV:                int32(contractCodeExtV),
		ContractCodeCode:                contractCodeCode,
		ContractCodeExpirationLedgerSeq: uint32(contractCodeExpirationLedgerSeq),
		LastModifiedLedger:              uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:               uint32(changeType),
		Deleted:                         outputDeleted,
	}
	return transformedPool, nil
}
