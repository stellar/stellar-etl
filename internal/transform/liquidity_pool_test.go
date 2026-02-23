package transform

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestTransformPool(t *testing.T) {
	type inputStruct struct {
		ingest ingest.Change
	}
	type transformTest struct {
		input      inputStruct
		wantOutput PoolOutput
		wantErr    error
	}

	hardCodedInput := makePoolTestInput()
	hardCodedOutput := makePoolTestOutput()

	tests := []transformTest{
		{
			inputStruct{
				ingest.Change{
					ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Type:       xdr.LedgerEntryTypeOffer,
					Pre:        nil,
					Post: &xdr.LedgerEntry{
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeOffer,
						},
					},
				},
			},
			PoolOutput{}, nil,
		},
		{
			inputStruct{
				hardCodedInput,
			},
			hardCodedOutput, nil,
		},
	}

	for _, test := range tests {
		header := xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				ScpValue: xdr.StellarValue{
					CloseTime: 1000,
				},
				LedgerSeq: 10,
			},
		}
		actualOutput, actualError := TransformPool(test.input.ingest, header)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makePoolTestInput() ingest.Change {
	ledgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 30705278,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeLiquidityPool,
			LiquidityPool: &xdr.LiquidityPoolEntry{
				LiquidityPoolId: xdr.PoolId{23, 45, 67},
				Body: xdr.LiquidityPoolEntryBody{
					Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
					ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
						Params: xdr.LiquidityPoolConstantProductParameters{
							AssetA: lpAssetA,
							AssetB: lpAssetB,
							Fee:    30,
						},
						ReserveA:                 105,
						ReserveB:                 10,
						TotalPoolShares:          35,
						PoolSharesTrustLineCount: 5,
					},
				},
			},
		},
	}
	return ingest.Change{
		ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		Type:       xdr.LedgerEntryTypeLiquidityPool,
		Pre:        &ledgerEntry,
		Post:       nil,
	}
}

func makePoolTestOutput() PoolOutput {
	return PoolOutput{
		PoolID:             "172d430000000000000000000000000000000000000000000000000000000000",
		PoolType:           "constant_product",
		PoolFee:            30,
		TrustlineCount:     5,
		PoolShareCount:     0.0000035,
		AssetAType:         "native",
		AssetACode:         lpAssetA.GetCode(),
		AssetAIssuer:       lpAssetA.GetIssuer(),
		AssetAID:           -5706705804583548011,
		AssetAReserve:      0.0000105,
		AssetBType:         "credit_alphanum4",
		AssetBCode:         lpAssetB.GetCode(),
		AssetBID:           6690054458235693884,
		AssetBIssuer:       lpAssetB.GetIssuer(),
		AssetBReserve:      0.0000010,
		LastModifiedLedger: 30705278,
		LedgerEntryChange:  2,
		Deleted:            true,
		LedgerSequence:     10,
		ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
		PoolIDStrkey:       "LALS2QYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC2X",
	}
}
