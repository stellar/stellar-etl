package utils

import (
	"encoding/hex"
	"errors"
	"math/big"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

//PanicOnError is a function that panics if the provided error is not nil
func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

//HashToHexString is utility function that converts and xdr.Hash type to a hex string
func HashToHexString(inputHash xdr.Hash) string {
	sliceHash := inputHash[:]
	hexString := hex.EncodeToString(sliceHash)
	return hexString
}

//TimePointToUTCTimeStamp takes in an xdr TimePoint and converts it to a time.Time struct in UTC. It returns an error for negative timepoints
func TimePointToUTCTimeStamp(providedTime xdr.TimePoint) (time.Time, error) {
	intTime := int64(providedTime)
	if intTime < 0 {
		return time.Now(), errors.New("The timepoint is negative")
	}
	return time.Unix(intTime, 0).UTC(), nil
}

//GetAccountAddressFromMuxedAccount takes in a muxed account and returns the address of the account
func GetAccountAddressFromMuxedAccount(account xdr.MuxedAccount) (string, error) {
	providedID := account.ToAccountId()
	pointerToID := &providedID
	return pointerToID.GetAddress()
}

//CreateSampleTx creates a transaction with a single operation (BumpSequence), the min base fee, and infinite timebounds
func CreateSampleTx(sequence int64) xdr.TransactionEnvelope {
	kp, err := keypair.Random()
	PanicOnError(err)

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
	PanicOnError(err)

	env, err := tx.TxEnvelope()
	PanicOnError(err)
	return env
}

//ConvertStroopValueToReal converts a value in stroops, the smallest amount unit, into real units
func ConvertStroopValueToReal(input xdr.Int64) float64 {
	output, _ := big.NewRat(int64(input), int64(10000000)).Float64()
	return output
}

//CreateSampleResultMeta creates Transaction results with the desired success flag and number of sub operation results
func CreateSampleResultMeta(successful bool, subOperationCount int) xdr.TransactionResultMeta {
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
