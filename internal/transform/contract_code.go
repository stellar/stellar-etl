package transform

import (
	"encoding/base64"
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformContractCode converts a contract code ledger change entry into a form suitable for BigQuery
func TransformContractCode(ledgerChange ingest.Change, header xdr.LedgerHeaderHistoryEntry) (ContractCodeOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return ContractCodeOutput{}, err
	}

	contractCode, ok := ledgerEntry.Data.GetContractCode()
	if !ok {
		return ContractCodeOutput{}, fmt.Errorf("Could not extract contract code from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	// LedgerEntryChange must contain a contract code change to be parsed, otherwise skip
	if ledgerEntry.Data.Type != xdr.LedgerEntryTypeContractCode {
		return ContractCodeOutput{}, nil
	}

	contractCodeExtV := contractCode.Ext.V

	hashBinary, err := contractCode.Hash.MarshalBinary()
	if err != nil {
		return ContractCodeOutput{}, fmt.Errorf("Could not extract Val from contractData")
	}
	contractCodeHash := base64.StdEncoding.EncodeToString(hashBinary)

	contractCodeEntryBodyType := contractCode.Body.BodyType.String()
	// NOTE: Most likely don't need the binary code to be saved in BQ
	//binaryCode, err := contractCode.Body.MarshalBinary()
	//if err != nil {
	//	return ContractCodeOutput{}, fmt.Errorf("Could not extract Val from contractData")
	//}
	//contractCodeCode := base64.StdEncoding.EncodeToString(binaryCode)

	contractCodeExpirationLedgerSeq := contractCode.ExpirationLedgerSeq

	var outputDeletedAtLedger uint32
	if outputDeleted {
		outputDeletedAtLedger = uint32(header.Header.LedgerSeq)
	}

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(header.Header.ScpValue.CloseTime)
	if err != nil {
		return ContractCodeOutput{}, fmt.Errorf("for ledger %d: %v", header.Header.LedgerSeq, err)
	}

	transformedPool := ContractCodeOutput{
		ContractCodeHash:                contractCodeHash,
		ContractCodeExtV:                int32(contractCodeExtV),
		ContractCodeExpirationLedgerSeq: uint32(contractCodeExpirationLedgerSeq),
		ContractCodeEntryBodyType:       contractCodeEntryBodyType,
		LastModifiedLedger:              uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:               uint32(changeType),
		Deleted:                         outputDeleted,
		DeletedAtLedger:                 outputDeletedAtLedger,
		LedgerClosedAt:                  outputCloseTime,
		//ContractCodeCode:                contractCodeCode,
	}
	return transformedPool, nil
}
