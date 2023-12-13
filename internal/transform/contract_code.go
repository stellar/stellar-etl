package transform

import (
	"fmt"

	"github.com/stellar/go/hash"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
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

	ledgerKey, _ := ledgerEntry.LedgerKey()
	ledgerKeyByte, _ := ledgerKey.MarshalBinary()
	hashedLedgerKeyByte := hash.Hash(ledgerKeyByte)
	ledgerKeyHash, _ := strkey.Encode(strkey.VersionByteContract, hashedLedgerKeyByte[:])

	contractCodeExtV := contractCode.Ext.V

	contractCodeHashByte, _ := contractCode.Hash.MarshalBinary()
	contractCodeHash, _ := strkey.Encode(strkey.VersionByteContract, contractCodeHashByte)

	closedAt, err := utils.TimePointToUTCTimeStamp(header.Header.ScpValue.CloseTime)
	if err != nil {
		return ContractCodeOutput{}, err
	}

	ledgerSequence := header.Header.LedgerSeq

	transformedCode := ContractCodeOutput{
		ContractCodeHash:   contractCodeHash,
		ContractCodeExtV:   int32(contractCodeExtV),
		LastModifiedLedger: uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:  uint32(changeType),
		Deleted:            outputDeleted,
		ClosedAt:           closedAt,
		LedgerSequence:     uint32(ledgerSequence),
		LedgerKeyHash:      ledgerKeyHash,
	}
	return transformedCode, nil
}
