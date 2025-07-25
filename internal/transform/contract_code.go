package transform

import (
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// TransformContractCode converts a contract code ledger change entry into a form suitable for BigQuery
func TransformContractCode(ledgerChange ingest.Change, header xdr.LedgerHeaderHistoryEntry) (ContractCodeOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return ContractCodeOutput{}, err
	}

	contractCode, ok := ledgerEntry.Data.GetContractCode()
	if !ok {
		return ContractCodeOutput{}, fmt.Errorf("could not extract contract code from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	// LedgerEntryChange must contain a contract code change to be parsed, otherwise skip
	if ledgerEntry.Data.Type != xdr.LedgerEntryTypeContractCode {
		return ContractCodeOutput{}, nil
	}

	var ledgerKey xdr.LedgerKey
	var ledgerKeyHashBase64 string

	ledgerKeyHash := utils.LedgerEntryToLedgerKeyHash(ledgerEntry)

	ledgerKey, err = ledgerEntry.LedgerKey()
	if err != nil {
		return ContractCodeOutput{}, err
	}

	ledgerKeyHashBase64, err = xdr.MarshalBase64(ledgerKey)
	if err != nil {
		return ContractCodeOutput{}, err
	}

	contractCodeExtV := contractCode.Ext.V

	contractCodeHash := contractCode.Hash.HexString()

	closedAt, err := utils.TimePointToUTCTimeStamp(header.Header.ScpValue.CloseTime)
	if err != nil {
		return ContractCodeOutput{}, err
	}

	ledgerSequence := header.Header.LedgerSeq

	var outputNInstructions uint32
	var outputNFunctions uint32
	var outputNGlobals uint32
	var outputNTableEntries uint32
	var outputNTypes uint32
	var outputNDataSegments uint32
	var outputNElemSegments uint32
	var outputNImports uint32
	var outputNExports uint32
	var outputNDataSegmentBytes uint32

	extV1, ok := contractCode.Ext.GetV1()
	if ok {
		outputNInstructions = uint32(extV1.CostInputs.NInstructions)
		outputNFunctions = uint32(extV1.CostInputs.NFunctions)
		outputNGlobals = uint32(extV1.CostInputs.NGlobals)
		outputNTableEntries = uint32(extV1.CostInputs.NTableEntries)
		outputNTypes = uint32(extV1.CostInputs.NTypes)
		outputNDataSegments = uint32(extV1.CostInputs.NDataSegments)
		outputNElemSegments = uint32(extV1.CostInputs.NElemSegments)
		outputNImports = uint32(extV1.CostInputs.NImports)
		outputNExports = uint32(extV1.CostInputs.NExports)
		outputNDataSegmentBytes = uint32(extV1.CostInputs.NDataSegmentBytes)
	}

	transformedCode := ContractCodeOutput{
		ContractCodeHash:    contractCodeHash,
		ContractCodeExtV:    int32(contractCodeExtV),
		LastModifiedLedger:  uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:   uint32(changeType),
		Deleted:             outputDeleted,
		ClosedAt:            closedAt,
		LedgerSequence:      uint32(ledgerSequence),
		LedgerKeyHash:       ledgerKeyHash,
		NInstructions:       outputNInstructions,
		NFunctions:          outputNFunctions,
		NGlobals:            outputNGlobals,
		NTableEntries:       outputNTableEntries,
		NTypes:              outputNTypes,
		NDataSegments:       outputNDataSegments,
		NElemSegments:       outputNElemSegments,
		NImports:            outputNImports,
		NExports:            outputNExports,
		NDataSegmentBytes:   outputNDataSegmentBytes,
		LedgerKeyHashBase64: ledgerKeyHashBase64,
	}
	return transformedCode, nil
}
