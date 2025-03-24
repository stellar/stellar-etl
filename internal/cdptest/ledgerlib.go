package cdptest

import (
	"fmt"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

func LedgerSequence(lcm xdr.LedgerCloseMeta) (*uint32, error) {
	ledgerSequence := lcm.LedgerSequence()
	return &ledgerSequence, nil
}

func CloseTime(lcm xdr.LedgerCloseMeta) (*time.Time, error) {
	ledgerHeader := lcm.LedgerHeaderHistoryEntry()
	closeTime, err := utils.TimePointToUTCTimeStamp(ledgerHeader.Header.ScpValue.CloseTime)
	if err != nil {
		return nil, err
	}

	return &closeTime, nil
}

func BaseFee(lcm xdr.LedgerCloseMeta) (*uint32, error) {
	ledgerHeader := lcm.LedgerHeaderHistoryEntry()
	baseFee := uint32(ledgerHeader.Header.BaseFee)
	return &baseFee, nil
}

func BaseReserve(lcm xdr.LedgerCloseMeta) (*uint32, error) {
	ledgerHeader := lcm.LedgerHeaderHistoryEntry()
	baseReserve := uint32(ledgerHeader.Header.BaseReserve)
	return &baseReserve, nil
}

func SorobanFeeWrite1Kb(lcm xdr.LedgerCloseMeta) (*int64, error) {
	switch lcm.V {
	case 0:
		return nil, nil
	case 1:
		lcmV1Ext := lcm.MustV1().Ext
		switch lcmV1Ext.V {
		case 0:
			return nil, nil
		case 1:
			ext := lcmV1Ext.MustV1()
			sorobanFreeWrite1Kb := int64(ext.SorobanFeeWrite1Kb)
			return &sorobanFreeWrite1Kb, nil
		default:
			panic(fmt.Errorf("unsupported LedgerCloseMeta.V1.Ext.V: %d", lcmV1Ext.V))
		}
	default:
		panic(fmt.Errorf("unsupported LedgerCloseMeta.V: %d", lcm.V))
	}
}

func TotalByteSizeOfBucketList(lcm xdr.LedgerCloseMeta) (*uint64, error) {
	switch lcm.V {
	case 0:
		return nil, nil
	case 1:
		lcmV1 := lcm.MustV1()
		totalByteSizeOfBucketList := uint64(lcmV1.TotalByteSizeOfBucketList)
		return &totalByteSizeOfBucketList, nil
	default:
		panic(fmt.Sprintf("Unsupported LedgerCloseMeta.V: %d", lcm.V))
	}
}
