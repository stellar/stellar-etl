package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestTransformLedger(t *testing.T) {
	type transformTest struct {
		input      xdr.LedgerCloseMeta
		wantOutput LedgerOutput
		wantErr    error
	}
	hardCodedTxSet := xdr.TransactionSet{
		Txs: []xdr.TransactionEnvelope{
			createSampleTx(0),
			createSampleTx(1),
		},
	}
	hardCodedTxProcessing := []xdr.TransactionResultMeta{
		createSampleResultMeta(false, 3),
		createSampleResultMeta(true, 10),
	}
	hardCodedLedger, _ := xdr.NewLedgerCloseMeta(0, xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				LedgerSeq:          30578981,
				TotalCoins:         1054439020873472865,
				FeePool:            18153766209161,
				BaseFee:            100,
				BaseReserve:        5000000,
				MaxTxSetSize:       1000,
				LedgerVersion:      13,
				PreviousLedgerHash: xdr.Hash{0xf6, 0x3c, 0x15, 0xd0, 0xea, 0xf4, 0x8a, 0xfb, 0xd7, 0x51, 0xa4, 0xc4, 0xdf, 0xad, 0xe5, 0x4a, 0x34, 0x48, 0x5, 0x3c, 0x47, 0xc5, 0xa7, 0x1d, 0x62, 0x26, 0x68, 0xae, 0xc, 0xc2, 0xa2, 0x8},
				ScpValue:           xdr.StellarValue{CloseTime: 1594584547},
			},
			Hash: xdr.Hash{0x26, 0x93, 0x2d, 0xc4, 0xd8, 0x4b, 0x5f, 0xab, 0xe9, 0xae, 0x74, 0x4c, 0xb4, 0x3c, 0xe4, 0xc6, 0xda, 0xcc, 0xf9, 0x8c, 0x86, 0xa9, 0x91, 0xb2, 0xa1, 0x49, 0x45, 0xb1, 0xad, 0xac, 0x4d, 0x59},
		},
		TxSet:        hardCodedTxSet,
		TxProcessing: hardCodedTxProcessing,
	})
	correctTime, err := time.Parse("2006-1-2 15:04:05 MST", "2020-07-12 20:09:07 UTC")
	utils.PanicOnError(err)
	correctHeader, _ := xdr.MarshalBase64(hardCodedLedger.MustV0().LedgerHeader.Header)
	correctBytes := []byte(correctHeader)
	tests := []transformTest{
		{
			wrapLedgerHeader(xdr.LedgerHeader{
				TotalCoins: -1,
			}),
			LedgerOutput{},
			fmt.Errorf("The total number of coins (-1) is negative for ledger 0"),
		},
		{
			wrapLedgerHeader(xdr.LedgerHeader{
				FeePool: -1,
			}),
			LedgerOutput{},
			fmt.Errorf("The fee pool (-1) is negative for ledger 0"),
		},
		{
			wrapLedgerHeaderWithTransactions(xdr.LedgerHeader{
				MaxTxSetSize: 0,
			}, 2),
			LedgerOutput{},
			fmt.Errorf("The number of transactions and results are different (2 != 0)"),
		},
		{
			hardCodedLedger,
			LedgerOutput{
				Sequence:           int32(30578981),
				LedgerHash:         "26932dc4d84b5fabe9ae744cb43ce4c6daccf98c86a991b2a14945b1adac4d59",
				PreviousLedgerHash: "f63c15d0eaf48afbd751a4c4dfade54a3448053c47c5a71d622668ae0cc2a208",
				LedgerHeader:       correctBytes,
				ClosedAt:           correctTime,

				TotalCoins:      1054439020873472865,
				FeePool:         18153766209161,
				BaseFee:         100,
				BaseReserve:     5000000,
				MaxTxSetSize:    1000,
				ProtocolVersion: 13,

				TransactionCount:           1,
				OperationCount:             10,
				SuccessfulTransactionCount: 1,
				FailedTransactionCount:     1,
				TxSetOperationCount:        "13",
			},
			nil,
		},
	}

	for _, test := range tests {
		actualOutput, actualError := TransformLedger(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func wrapLedgerHeaderWithTransactions(header xdr.LedgerHeader, numTransactions int) xdr.LedgerCloseMeta {
	transactionEnvelopes := []xdr.TransactionEnvelope{}
	for txNum := 0; txNum < numTransactions; txNum++ {
		transactionEnvelopes = append(transactionEnvelopes, createSampleTx(uint32(txNum)))
	}
	lcm, err := xdr.NewLedgerCloseMeta(0, xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: header,
		},
		TxSet: xdr.TransactionSet{Txs: transactionEnvelopes},
	})
	utils.PanicOnError(err)
	return lcm
}

func wrapLedgerHeader(header xdr.LedgerHeader) xdr.LedgerCloseMeta {
	lcm, err := xdr.NewLedgerCloseMeta(0, xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: header,
		}})
	utils.PanicOnError(err)
	return lcm
}

func createSampleTx(sequence uint32) xdr.TransactionEnvelope {
	kp, err := keypair.Random()
	utils.PanicOnError(err)

	sourceAccount := txnbuild.NewSimpleAccount(kp.Address(), int64(0))
	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &sourceAccount,
			Operations: []txnbuild.Operation{
				&txnbuild.BumpSequence{
					BumpTo: int64(sequence),
				},
			},
			BaseFee:    txnbuild.MinBaseFee,
			Timebounds: txnbuild.NewInfiniteTimeout(),
		},
	)
	utils.PanicOnError(err)

	env, err := tx.TxEnvelope()
	utils.PanicOnError(err)
	return env
}

func createSampleResultMeta(successful bool, subOperationCount int) xdr.TransactionResultMeta {
	resultCode := xdr.TransactionResultCodeTxFailed
	if successful {
		resultCode = xdr.TransactionResultCodeTxSuccess
	}
	operationResults := []xdr.OperationResult{}
	for i := 0; i < subOperationCount; i++ {
		operationResults = append(operationResults, xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr:   &xdr.OperationResultTr{},
		})
	}
	return xdr.TransactionResultMeta{
		Result: xdr.TransactionResultPair{
			Result: xdr.TransactionResult{
				Result: xdr.TransactionResultResult{
					Code:    resultCode,
					Results: &operationResults,
				},
			},
		},
	}
}
