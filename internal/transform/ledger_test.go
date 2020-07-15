package transform

import (
<<<<<<< HEAD
	"errors"
=======
	"fmt"
>>>>>>> master
	"testing"
	"time"

	"github.com/stellar/stellar-etl/internal/utils"

<<<<<<< HEAD
	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

func createTestArchiveBackend() *ledgerbackend.HistoryArchiveBackend {
	archiveStellarURL := "http://history.stellar.org/prd/core-live/core_live_001"
	backend, err := ledgerbackend.NewHistoryArchiveBackendFromURL(archiveStellarURL)
	utils.PanicOnError(err)
	return backend
}
func TestTransformOnTrueLedger(t *testing.T) {
	sequenceNumber := uint32(30578981)
	backend := createTestArchiveBackend()
	defer backend.Close()

	ok, testLedger, err := backend.GetLedger(sequenceNumber)
	utils.PanicOnError(err)
	if !ok {
		panic("Ledger does not exist")
	}
	encodedLedgerHeader := "AAAADfY8FdDq9Ir711GkxN+t5Uo0SAU8R8WnHWImaK4MwqIIkvQSArXhiHcF4mc5dW+oX7J45SX+kVIw1fZFQOgAGUEAAAAAXwtt4wAAAAAAAAAAglgbxuZna3ZZPDE9cmMZZu4hr7EKrE9oMaGRPu2QqnhEVa9EjsMjpKJnOr9UnDpix+BSese+YEEnMYBllAoQYAHSmSUOoh6z7HlbYQAAEILAyUaJAAABFgAAAAAPNsPXAAAAZABMS0AAAAPomTlchfG4kTZ/6o0otCLK44Twu6KISQjni2rUp4dG8r+uRv1ZyQAYHHxS1EDGrx4+4hJAXwwfQZIiQ02F7J/5e/MWwpi1ImmLz+kwObksbcGQNVo7wwrC65BaoDpSpMXAeBTzHRcYFBKkzYgNmed535NYefx2iBzvXOUuhydcWEwAAAAA"
	byteFormOfLedgerHeader := []byte(encodedLedgerHeader)
	parsedTime, err := time.Parse("2006-1-2 15:04:05 MST", "2020-07-12 20:09:07 UTC")
	desiredOutput := LedgerOutput{
		Sequence:           int32(sequenceNumber),
		LedgerHash:         "26932dc4d84b5fabe9ae744cb43ce4c6daccf98c86a991b2a14945b1adac4d59",
		PreviousLedgerHash: "f63c15d0eaf48afbd751a4c4dfade54a3448053c47c5a71d622668ae0cc2a208",
		LedgerHeader:       byteFormOfLedgerHeader,

		TransactionCount:           2,
		OperationCount:             6,
		SuccessfulTransactionCount: 2,
		FailedTransactionCount:     201,
		TxSetOperationCount:        "208",

		ClosedAt: parsedTime,

		TotalCoins:      1054439020873472865,
		FeePool:         18153766209161,
		BaseFee:         100,
		BaseReserve:     5000000,
		MaxTxSetSize:    1000,
		ProtocolVersion: 13,
	}
	convertedLedger, err := ConvertLedger(testLedger)
	assert.Equal(t, desiredOutput, convertedLedger)

}

func TestTransformLedger(t *testing.T) {
	type transformTest struct {
		input         xdr.LedgerCloseMeta
		desiredOutput LedgerOutput
		desiredError  error
	}

=======
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
>>>>>>> master
	tests := []transformTest{
		{
			wrapLedgerHeader(xdr.LedgerHeader{
				TotalCoins: -1,
			}),
			LedgerOutput{},
<<<<<<< HEAD
			errors.New("The total number of coins is a negative value"),
=======
			fmt.Errorf("The total number of coins (-1) is negative for ledger 0"),
>>>>>>> master
		},
		{
			wrapLedgerHeader(xdr.LedgerHeader{
				FeePool: -1,
			}),
			LedgerOutput{},
<<<<<<< HEAD
			errors.New("The fee pool is a negative value"),
=======
			fmt.Errorf("The fee pool (-1) is negative for ledger 0"),
>>>>>>> master
		},
		{
			wrapLedgerHeaderWithTransactions(xdr.LedgerHeader{
				MaxTxSetSize: 0,
			}, 2),
			LedgerOutput{},
<<<<<<< HEAD
			errors.New("The number of transactions and results are different"),
=======
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
>>>>>>> master
		},
	}

	for _, test := range tests {
<<<<<<< HEAD
		actualOutput, actualError := ConvertLedger(test.input)
		assert.Equal(t, test.desiredError, actualError)
		assert.Equal(t, test.desiredOutput, actualOutput)
=======
		actualOutput, actualError := TransformLedger(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
>>>>>>> master
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
<<<<<<< HEAD
	if err != nil {
		panic(err)
	}
=======
	utils.PanicOnError(err)
>>>>>>> master

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
<<<<<<< HEAD
	if err != nil {
		panic(err)
	}

	env, err := tx.TxEnvelope()
	if err != nil {
		panic(err)
	}
	return env
}
=======
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
>>>>>>> master
