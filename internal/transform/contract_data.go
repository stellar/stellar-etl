package transform

import (
	"encoding/base64"
	"fmt"
	"math/big"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

const (
	scDecimalPrecision = 7
)

var (
	// https://github.com/stellar/rs-soroban-env/blob/v0.0.16/soroban-env-host/src/native_contract/token/public_types.rs#L22
	nativeAssetSym = xdr.ScSymbol("Native")
	// these are storage DataKey enum
	// https://github.com/stellar/rs-soroban-env/blob/v0.0.16/soroban-env-host/src/native_contract/token/storage_types.rs#L23
	balanceMetadataSym = xdr.ScSymbol("Balance")
	metadataSym        = xdr.ScSymbol("METADATA")
	metadataNameSym    = xdr.ScSymbol("name")
	metadataSymbolSym  = xdr.ScSymbol("symbol")
	adminSym           = xdr.ScSymbol("Admin")
	issuerSym          = xdr.ScSymbol("issuer")
	assetCodeSym       = xdr.ScSymbol("asset_code")
	alphaNum4Sym       = xdr.ScSymbol("AlphaNum4")
	alphaNum12Sym      = xdr.ScSymbol("AlphaNum12")
	decimalSym         = xdr.ScSymbol("decimal")
	assetInfoSym       = xdr.ScSymbol("AssetInfo")
	decimalVal         = xdr.Uint32(scDecimalPrecision)
	assetInfoVec       = &xdr.ScVec{
		xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &assetInfoSym,
		},
	}
	assetInfoKey = xdr.ScVal{
		Type: xdr.ScValTypeScvVec,
		Vec:  &assetInfoVec,
	}
)

type AssetFromContractDataFunc func(ledgerEntry xdr.LedgerEntry, passphrase string) (string, string)
type ContractBalanceFromContractDataFunc func(ledgerEntry xdr.LedgerEntry, passphrase string) ([32]byte, *big.Int, bool)

type TransformContractDataStruct struct {
	AssetFromContractData           AssetFromContractDataFunc
	ContractBalanceFromContractData ContractBalanceFromContractDataFunc
}

func NewTransformContractDataStruct(assetfrom AssetFromContractDataFunc, contractBalance ContractBalanceFromContractDataFunc) *TransformContractDataStruct {
	return &TransformContractDataStruct{
		AssetFromContractData:           assetfrom,
		ContractBalanceFromContractData: contractBalance,
	}
}

// TransformContractData converts a contract data ledger change entry into a form suitable for BigQuery
func (t *TransformContractDataStruct) TransformContractData(ledgerChange ingest.Change, passphrase string, header xdr.LedgerHeaderHistoryEntry) (ContractDataOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return ContractDataOutput{}, err
	}

	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return ContractDataOutput{}, fmt.Errorf("Could not extract contract data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	// LedgerEntryChange must contain a contract data change to be parsed, otherwise skip
	if ledgerEntry.Data.Type != xdr.LedgerEntryTypeContractData {
		return ContractDataOutput{}, nil
	}

	contractDataAssetCode, contractDataAssetIssuer := t.AssetFromContractData(ledgerEntry, passphrase)

	contractDataBalanceHolder, contractDataBalance, _ := t.ContractBalanceFromContractData(ledgerEntry, passphrase)

	isNonce := false
	if contractData.Key.Type.String() == "ScValTypeScvLedgerKeyNonce" {
		isNonce = true
	}

	if isNonce {
		return ContractDataOutput{IsNonce: isNonce}, nil
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

	var outputDeletedAtLedger uint32
	if outputDeleted {
		outputDeletedAtLedger = uint32(header.Header.LedgerSeq)
	}

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(header.Header.ScpValue.CloseTime)
	if err != nil {
		return ContractDataOutput{}, fmt.Errorf("for ledger %d: %v", header.Header.LedgerSeq, err)
	}

	transformedPool := ContractDataOutput{
		ContractId:                  contractDataContractId.HexString(),
		ContractKey:                 contractDataKey,
		ContractDurability:          contractDataDurability,
		ContractDataFlags:           uint32(contractDataDataFlags),
		ContractDataVal:             contractDataDataVal,
		ContractExpirationLedgerSeq: uint32(contractDataExpirationLedgerSeq),
		ContractDataAssetCode:       contractDataAssetCode,
		ContractDataAssetIssuer:     contractDataAssetIssuer,
		ContractDataBalanceHolder:   base64.StdEncoding.EncodeToString(contractDataBalanceHolder[:]),
		ContractDataBalance:         contractDataBalance.String(),
		LastModifiedLedger:          uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:           uint32(changeType),
		Deleted:                     outputDeleted,
		DeletedAtLedger:             outputDeletedAtLedger,
		LedgerClosedAt:              outputCloseTime,
		IsNonce:                     isNonce,
	}
	return transformedPool, nil
}

// AssetFromContractData takes a ledger entry and verifies if the ledger entry
// corresponds to the asset info entry written to contract storage by the Stellar
// Asset Contract upon initialization.
//
// Note that AssetFromContractData will ignore forged asset info entries by
// deriving the Stellar Asset Contract ID from the asset info entry and comparing
// it to the contract ID found in the ledger entry.
//
// If the given ledger entry is a verified asset info entry,
// AssetFromContractData will return the corresponding Stellar asset. Otherwise,
// it returns empty strings.
//
// References:
// https://github.com/stellar/rs-soroban-env/blob/v0.0.16/soroban-env-host/src/native_contract/token/public_types.rs#L21
// https://github.com/stellar/rs-soroban-env/blob/v0.0.16/soroban-env-host/src/native_contract/token/asset_info.rs#L6
// https://github.com/stellar/rs-soroban-env/blob/v0.0.16/soroban-env-host/src/native_contract/token/contract.rs#L115
//
// The asset info in `ContractData` entry takes the following form:
//
//   - Instance storage - it's part of contract instance data storage
//
//   - Key: a vector with one element, which is the symbol "AssetInfo"
//
//     ScVal{ Vec: ScVec({ ScVal{ Sym: ScSymbol("AssetInfo") }})}
//
//   - Value: a map with two key-value pairs: code and issuer
//
//     ScVal{ Map: ScMap(
//     { ScVal{ Sym: ScSymbol("asset_code") } -> ScVal{ Str: ScString(...) } },
//     { ScVal{ Sym: ScSymbol("issuer") } -> ScVal{ Bytes: ScBytes(...) } }
//     )}
func AssetFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) (string, string) {
	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return "", ""
	}
	if contractData.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance ||
		contractData.Body.BodyType != xdr.ContractEntryBodyTypeDataEntry {
		return "", ""
	}
	contractInstanceData, ok := contractData.Body.Data.Val.GetInstance()
	if !ok || contractInstanceData.Storage == nil {
		return "", ""
	}

	// we don't support asset stats for lumens
	nativeAssetContractID, err := xdr.MustNewNativeAsset().ContractID(passphrase)
	if err != nil || (contractData.Contract.ContractId != nil && (*contractData.Contract.ContractId) == nativeAssetContractID) {
		return "", ""
	}

	var assetInfo *xdr.ScVal
	for _, mapEntry := range *contractInstanceData.Storage {
		if mapEntry.Key.Equals(assetInfoKey) {
			// clone the map entry to avoid reference to loop iterator
			mapValXdr, cloneErr := mapEntry.Val.MarshalBinary()
			if cloneErr != nil {
				return "", ""
			}
			assetInfo = &xdr.ScVal{}
			cloneErr = assetInfo.UnmarshalBinary(mapValXdr)
			if cloneErr != nil {
				return "", ""
			}
			break
		}
	}

	if assetInfo == nil {
		return "", ""
	}

	vecPtr, ok := assetInfo.GetVec()
	if !ok || vecPtr == nil || len(*vecPtr) != 2 {
		return "", ""
	}
	vec := *vecPtr

	sym, ok := vec[0].GetSym()
	if !ok {
		return "", ""
	}
	switch sym {
	case "AlphaNum4":
	case "AlphaNum12":
	default:
		return "", ""
	}

	var assetCode, assetIssuer string
	assetMapPtr, ok := vec[1].GetMap()
	if !ok || assetMapPtr == nil || len(*assetMapPtr) != 2 {
		return "", ""
	}
	assetMap := *assetMapPtr

	assetCodeEntry, assetIssuerEntry := assetMap[0], assetMap[1]
	if sym, ok = assetCodeEntry.Key.GetSym(); !ok || sym != assetCodeSym {
		return "", ""
	}
	assetCodeSc, ok := assetCodeEntry.Val.GetStr()
	if !ok {
		return "", ""
	}
	if assetCode = string(assetCodeSc); assetCode == "" {
		return "", ""
	}

	if sym, ok = assetIssuerEntry.Key.GetSym(); !ok || sym != issuerSym {
		return "", ""
	}
	assetIssuerSc, ok := assetIssuerEntry.Val.GetBytes()
	if !ok {
		return "", ""
	}
	assetIssuer, err = strkey.Encode(strkey.VersionByteAccountID, assetIssuerSc)
	if err != nil {
		return "", ""
	}

	asset, err := xdr.NewCreditAsset(assetCode, assetIssuer)
	if err != nil {
		return "", ""
	}

	expectedID, err := asset.ContractID(passphrase)
	if err != nil {
		return "", ""
	}
	if contractData.Contract.ContractId == nil || expectedID != *(contractData.Contract.ContractId) {
		return "", ""
	}

	return assetCode, assetIssuer
}

// ContractBalanceFromContractData takes a ledger entry and verifies that the
// ledger entry corresponds to the balance entry written to contract storage by
// the Stellar Asset Contract.
//
// Reference:
//
//	https://github.com/stellar/rs-soroban-env/blob/da325551829d31dcbfa71427d51c18e71a121c5f/soroban-env-host/src/native_contract/token/storage_types.rs#L11-L24
func ContractBalanceFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) ([32]byte, *big.Int, bool) {
	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return [32]byte{}, nil, false
	}

	// we don't support asset stats for lumens
	nativeAssetContractID, err := xdr.MustNewNativeAsset().ContractID(passphrase)
	if err != nil || (contractData.Contract.ContractId != nil && *contractData.Contract.ContractId == nativeAssetContractID) {
		return [32]byte{}, nil, false
	}

	keyEnumVecPtr, ok := contractData.Key.GetVec()
	if !ok || keyEnumVecPtr == nil {
		return [32]byte{}, nil, false
	}
	keyEnumVec := *keyEnumVecPtr
	if len(keyEnumVec) != 2 || !keyEnumVec[0].Equals(
		xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &balanceMetadataSym,
		},
	) {
		return [32]byte{}, nil, false
	}

	scAddress, ok := keyEnumVec[1].GetAddress()
	if !ok {
		return [32]byte{}, nil, false
	}

	holder, ok := scAddress.GetContractId()
	if !ok {
		return [32]byte{}, nil, false
	}

	balanceMapPtr, ok := contractData.Body.Data.Val.GetMap()
	if !ok || balanceMapPtr == nil {
		return [32]byte{}, nil, false
	}
	balanceMap := *balanceMapPtr
	if !ok || len(balanceMap) != 3 {
		return [32]byte{}, nil, false
	}

	var keySym xdr.ScSymbol
	if keySym, ok = balanceMap[0].Key.GetSym(); !ok || keySym != "amount" {
		return [32]byte{}, nil, false
	}
	if keySym, ok = balanceMap[1].Key.GetSym(); !ok || keySym != "authorized" ||
		!balanceMap[1].Val.IsBool() {
		return [32]byte{}, nil, false
	}
	if keySym, ok = balanceMap[2].Key.GetSym(); !ok || keySym != "clawback" ||
		!balanceMap[2].Val.IsBool() {
		return [32]byte{}, nil, false
	}
	amount, ok := balanceMap[0].Val.GetI128()
	if !ok {
		return [32]byte{}, nil, false
	}

	// amount cannot be negative
	// https://github.com/stellar/rs-soroban-env/blob/a66f0815ba06a2f5328ac420950690fd1642f887/soroban-env-host/src/native_contract/token/balance.rs#L92-L93
	if int64(amount.Hi) < 0 {
		return [32]byte{}, nil, false
	}
	amt := new(big.Int).Lsh(new(big.Int).SetInt64(int64(amount.Hi)), 64)
	amt.Add(amt, new(big.Int).SetUint64(uint64(amount.Lo)))
	return holder, amt, true
}

func metadataObjFromAsset(isNative bool, code, issuer string) (*xdr.ScMap, error) {
	assetInfoVecKey := &xdr.ScVec{
		xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &assetInfoSym,
		},
	}

	if isNative {
		nativeVec := &xdr.ScVec{
			xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &nativeAssetSym,
			},
		}
		return &xdr.ScMap{
			xdr.ScMapEntry{
				Key: xdr.ScVal{
					Type: xdr.ScValTypeScvVec,
					Vec:  &assetInfoVecKey,
				},
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvVec,
					Vec:  &nativeVec,
				},
			},
		}, nil
	}

	nameVal := xdr.ScString(code + ":" + issuer)
	symbolVal := xdr.ScString(code)
	metaDataMap := &xdr.ScMap{
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &decimalSym,
			},
			Val: xdr.ScVal{
				Type: xdr.ScValTypeScvU32,
				U32:  &decimalVal,
			},
		},
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &metadataNameSym,
			},
			Val: xdr.ScVal{
				Type: xdr.ScValTypeScvString,
				Str:  &nameVal,
			},
		},
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &metadataSymbolSym,
			},
			Val: xdr.ScVal{
				Type: xdr.ScValTypeScvString,
				Str:  &symbolVal,
			},
		},
	}

	adminVec := &xdr.ScVec{
		xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &adminSym,
		},
	}

	adminAccountId := xdr.MustAddress(issuer)
	assetCodeVal := xdr.ScString(code)
	issuerBytes, err := strkey.Decode(strkey.VersionByteAccountID, issuer)
	if err != nil {
		return nil, err
	}

	assetIssuerBytes := xdr.ScBytes(issuerBytes)
	assetInfoMap := &xdr.ScMap{
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &assetCodeSym,
			},
			Val: xdr.ScVal{
				Type: xdr.ScValTypeScvString,
				Str:  &assetCodeVal,
			},
		},
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &issuerSym,
			},
			Val: xdr.ScVal{
				Type:  xdr.ScValTypeScvBytes,
				Bytes: &assetIssuerBytes,
			},
		},
	}

	alphaNumSym := alphaNum4Sym
	if len(code) > 4 {
		alphaNumSym = alphaNum12Sym
	}
	assetInfoVecVal := &xdr.ScVec{
		xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &alphaNumSym,
		},
		xdr.ScVal{
			Type: xdr.ScValTypeScvMap,
			Map:  &assetInfoMap,
		},
	}

	storageMap := &xdr.ScMap{
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &metadataSym,
			},
			Val: xdr.ScVal{
				Type: xdr.ScValTypeScvMap,
				Map:  &metaDataMap,
			},
		},
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec:  &adminVec,
			},
			Val: xdr.ScVal{
				Type: xdr.ScValTypeScvAddress,
				Address: &xdr.ScAddress{
					AccountId: &adminAccountId,
				},
			},
		},
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec:  &assetInfoVecKey,
			},
			Val: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec:  &assetInfoVecVal,
			},
		},
	}

	return storageMap, nil
}
