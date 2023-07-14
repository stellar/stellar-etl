package transform

import (
	"encoding/base64"
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformContractData converts a contract data ledger change entry into a form suitable for BigQuery
func TransformContractData(ledgerChange ingest.Change) (ContractDataOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return ContractDataOutput{}, err
	}

	// LedgerEntryChange must contain a contract data change to be parsed, otherwise skip
	if ledgerEntry.Data.Type != xdr.LedgerEntryTypeContractData {
		return ContractDataOutput{}, nil
	}

	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return ContractDataOutput{}, fmt.Errorf("Could not extract contract data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	contractDataContractId, ok := contractData.Contract.GetContractId()
	if !ok {
		return ContractDataOutput{}, fmt.Errorf("Could not extract contractId data information from contractData")
	}

	keyBinary, err := contractData.Key.MarshalBinary()
	if err != nil {
		return ContractDataOutput{}, fmt.Errorf("Could not extract Key from contractData")
	}
	contractDataKey := base64.StdEncoding.EncodeToString(keyBinary)

	contractDataDurability := contractData.Durability.String()

	contractDataData, ok := contractData.Body.GetData()
	if !ok {
		return ContractDataOutput{}, fmt.Errorf("Could not extract contract data information from contractId %s", xdr.Hash(*contractData.Contract.ContractId).HexString())
	}
	contractDataDataFlags := contractDataData.Flags

	valBinary, err := contractDataData.Val.MarshalBinary()
	if err != nil {
		return ContractDataOutput{}, fmt.Errorf("Could not extract Val from contractData")
	}
	contractDataDataVal := base64.StdEncoding.EncodeToString(valBinary)

	contractDataExpirationLedgerSeq := contractData.ExpirationLedgerSeq

	transformedPool := ContractDataOutput{
		ContractId:                  contractDataContractId.HexString(),
		ContractKey:                 contractDataKey,
		ContractDurability:          contractDataDurability,
		ContractDataFlags:           uint32(contractDataDataFlags),
		ContractDataVal:             contractDataDataVal,
		ContractExpirationLedgerSeq: uint32(contractDataExpirationLedgerSeq),
		LastModifiedLedger:          uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:           uint32(changeType),
		Deleted:                     outputDeleted,
	}
	return transformedPool, nil
}
