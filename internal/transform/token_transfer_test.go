package transform

import (
	"testing"
	"time"

	"github.com/guregu/null"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/asset"
	"github.com/stellar/go/processors/token_transfer"
	"github.com/stellar/go/xdr"
)

func TestTransformTokenTransfer(t *testing.T) {
	type inputStruct struct {
		events []*token_transfer.TokenTransferEvent
		lcm    xdr.LedgerCloseMeta
	}
	type transformTest struct {
		input      inputStruct
		wantOutput []TokenTransferOutput
		wantErr    error
	}

	var err error
	var hardCodedEvents [][]*token_transfer.TokenTransferEvent
	var hardCodedLCM []xdr.LedgerCloseMeta
	var hardCodedOutput [][]TokenTransferOutput
	var tests []transformTest

	hardCodedEvents, hardCodedLCM, err = makeTokenTransferTestInput()
	assert.NoError(t, err)
	hardCodedOutput, err = makeTokenTransferTestOutput()
	assert.NoError(t, err)

	for i := range hardCodedEvents {
		tests = append(tests, transformTest{
			input:      inputStruct{hardCodedEvents[i], hardCodedLCM[i]},
			wantOutput: hardCodedOutput[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := transformEvents(test.input.events, test.input.lcm)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeTokenTransferTestOutput() (output [][]TokenTransferOutput, err error) {
	output = [][]TokenTransferOutput{
		{
			{
				TransactionHash: "txhash",
				TransactionID:   42949677056,
				OperationID:     null.IntFrom(42949677057),
				EventTopic:      "transfer",
				From:            null.StringFrom("from"),
				To:              null.StringFrom("to"),
				Asset:           "credit_alphanum4:abc:def",
				AssetType:       "credit_alphanum4",
				AssetCode:       null.StringFrom("abc"),
				AssetIssuer:     null.StringFrom("def"),
				Amount:          9.999999999999999e-06,
				AmountRaw:       "100",
				ContractID:      "contractaddress",
				LedgerSequence:  uint32(10),
				ClosedAt:        time.Unix(1000, 0).UTC(),
				ToMuxed:         null.NewString("", false),
				ToMuxedID:       null.NewString("", false),
			},
			{
				TransactionHash: "txhash",
				TransactionID:   42949677056,
				OperationID:     null.IntFrom(42949677057),
				EventTopic:      "mint",
				From:            null.NewString("", false),
				To:              null.StringFrom("to"),
				Asset:           "credit_alphanum4:abc:def",
				AssetType:       "credit_alphanum4",
				AssetCode:       null.StringFrom("abc"),
				AssetIssuer:     null.StringFrom("def"),
				Amount:          9.999999999999999e-06,
				AmountRaw:       "100",
				ContractID:      "contractaddress",
				LedgerSequence:  uint32(10),
				ClosedAt:        time.Unix(1000, 0).UTC(),
				ToMuxed:         null.NewString("", false),
				ToMuxedID:       null.NewString("", false),
			},
			{
				TransactionHash: "txhash",
				TransactionID:   42949677056,
				OperationID:     null.IntFrom(42949677057),
				EventTopic:      "burn",
				From:            null.StringFrom("from"),
				To:              null.NewString("", false),
				Asset:           "credit_alphanum4:abc:def",
				AssetType:       "credit_alphanum4",
				AssetCode:       null.StringFrom("abc"),
				AssetIssuer:     null.StringFrom("def"),
				Amount:          9.999999999999999e-06,
				AmountRaw:       "100",
				ContractID:      "contractaddress",
				LedgerSequence:  uint32(10),
				ClosedAt:        time.Unix(1000, 0).UTC(),
				ToMuxed:         null.NewString("", false),
				ToMuxedID:       null.NewString("", false),
			},
			{
				TransactionHash: "txhash",
				TransactionID:   42949677056,
				OperationID:     null.IntFrom(42949677057),
				EventTopic:      "clawback",
				From:            null.StringFrom("from"),
				To:              null.NewString("", false),
				Asset:           "credit_alphanum4:abc:def",
				AssetType:       "credit_alphanum4",
				AssetCode:       null.StringFrom("abc"),
				AssetIssuer:     null.StringFrom("def"),
				Amount:          9.999999999999999e-06,
				AmountRaw:       "100",
				ContractID:      "contractaddress",
				LedgerSequence:  uint32(10),
				ClosedAt:        time.Unix(1000, 0).UTC(),
				ToMuxed:         null.NewString("", false),
				ToMuxedID:       null.NewString("", false),
			},
			{
				TransactionHash: "txhash",
				TransactionID:   42949677056,
				OperationID:     null.IntFrom(42949677057),
				EventTopic:      "fee",
				From:            null.StringFrom("from"),
				To:              null.NewString("", false),
				Asset:           "credit_alphanum4:abc:def",
				AssetType:       "credit_alphanum4",
				AssetCode:       null.StringFrom("abc"),
				AssetIssuer:     null.StringFrom("def"),
				Amount:          9.999999999999999e-06,
				AmountRaw:       "100",
				ContractID:      "contractaddress",
				LedgerSequence:  uint32(10),
				ClosedAt:        time.Unix(1000, 0).UTC(),
				ToMuxed:         null.NewString("", false),
				ToMuxedID:       null.NewString("", false),
			},
		},
	}

	return
}

func makeTokenTransferTestInput() (events [][]*token_transfer.TokenTransferEvent, ledgers []xdr.LedgerCloseMeta, err error) {
	operationIndex := uint32(1)

	events = [][]*token_transfer.TokenTransferEvent{
		{
			{
				Meta: &token_transfer.EventMeta{
					LedgerSequence:   10,
					TxHash:           "txhash",
					TransactionIndex: 1,
					OperationIndex:   &operationIndex,
					ContractAddress:  "contractaddress",
				},
				Event: &token_transfer.TokenTransferEvent_Transfer{
					Transfer: &token_transfer.Transfer{
						From: "from",
						To:   "to",
						Asset: &asset.Asset{
							AssetType: &asset.Asset_IssuedAsset{
								IssuedAsset: &asset.IssuedAsset{
									AssetCode: "abc",
									Issuer:    "def",
								},
							},
						},
						Amount: "100",
					},
				},
			},
			{
				Meta: &token_transfer.EventMeta{
					LedgerSequence:   10,
					TxHash:           "txhash",
					TransactionIndex: 1,
					OperationIndex:   &operationIndex,
					ContractAddress:  "contractaddress",
				},
				Event: &token_transfer.TokenTransferEvent_Mint{
					Mint: &token_transfer.Mint{
						To: "to",
						Asset: &asset.Asset{
							AssetType: &asset.Asset_IssuedAsset{
								IssuedAsset: &asset.IssuedAsset{
									AssetCode: "abc",
									Issuer:    "def",
								},
							},
						},
						Amount: "100",
					},
				},
			},
			{
				Meta: &token_transfer.EventMeta{
					LedgerSequence:   10,
					TxHash:           "txhash",
					TransactionIndex: 1,
					OperationIndex:   &operationIndex,
					ContractAddress:  "contractaddress",
				},
				Event: &token_transfer.TokenTransferEvent_Burn{
					Burn: &token_transfer.Burn{
						From: "from",
						Asset: &asset.Asset{
							AssetType: &asset.Asset_IssuedAsset{
								IssuedAsset: &asset.IssuedAsset{
									AssetCode: "abc",
									Issuer:    "def",
								},
							},
						},
						Amount: "100",
					},
				},
			},
			{
				Meta: &token_transfer.EventMeta{
					LedgerSequence:   10,
					TxHash:           "txhash",
					TransactionIndex: 1,
					OperationIndex:   &operationIndex,
					ContractAddress:  "contractaddress",
				},
				Event: &token_transfer.TokenTransferEvent_Clawback{
					Clawback: &token_transfer.Clawback{
						From: "from",
						Asset: &asset.Asset{
							AssetType: &asset.Asset_IssuedAsset{
								IssuedAsset: &asset.IssuedAsset{
									AssetCode: "abc",
									Issuer:    "def",
								},
							},
						},
						Amount: "100",
					},
				},
			},
			{
				Meta: &token_transfer.EventMeta{
					LedgerSequence:   10,
					TxHash:           "txhash",
					TransactionIndex: 1,
					OperationIndex:   &operationIndex,
					ContractAddress:  "contractaddress",
				},
				Event: &token_transfer.TokenTransferEvent_Fee{
					Fee: &token_transfer.Fee{
						From: "from",
						Asset: &asset.Asset{
							AssetType: &asset.Asset_IssuedAsset{
								IssuedAsset: &asset.IssuedAsset{
									AssetCode: "abc",
									Issuer:    "def",
								},
							},
						},
						Amount: "100",
					},
				},
			},
		},
	}

	ledgers = []xdr.LedgerCloseMeta{
		{
			V: 1,
			V1: &xdr.LedgerCloseMetaV1{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						ScpValue: xdr.StellarValue{
							CloseTime: 1000,
						},
						LedgerSeq: 10,
					},
				},
			},
		},
		{
			V: 1,
			V1: &xdr.LedgerCloseMetaV1{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						ScpValue: xdr.StellarValue{
							CloseTime: 1000,
						},
						LedgerSeq: 10,
					},
				},
			},
		},
		{
			V: 1,
			V1: &xdr.LedgerCloseMetaV1{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						ScpValue: xdr.StellarValue{
							CloseTime: 1000,
						},
						LedgerSeq: 10,
					},
				},
			},
		},
		{
			V: 1,
			V1: &xdr.LedgerCloseMetaV1{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						ScpValue: xdr.StellarValue{
							CloseTime: 1000,
						},
						LedgerSeq: 10,
					},
				},
			},
		},
		{
			V: 2,
			V2: &xdr.LedgerCloseMetaV2{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						ScpValue: xdr.StellarValue{
							CloseTime: 1000,
						},
						LedgerSeq: 10,
					},
				},
			},
		},
	}
	return
}
