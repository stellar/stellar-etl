package utils

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/spf13/pflag"
	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
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

// AddBasicFlags adds the start-ledger, end-ledger, limit, output, and stdout flags to the provided flagset
func AddBasicFlags(objectName string, flags *pflag.FlagSet) {
	flags.Uint32P("start-ledger", "s", 1, "The ledger sequence number for the beginning of the export period. Defaults to genesis ledger")
	flags.Uint32P("end-ledger", "e", 0, "The ledger sequence number for the end of the export range (required)")
	flags.Int64P("limit", "l", -1, "Maximum number of "+objectName+" to export. If the limit is set to a negative number, all the objects in the provided range are exported")
	flags.StringP("output", "o", "exported_"+objectName+".txt", "Filename of the output file")
	flags.Bool("stdout", false, "If set, the output will be printed to stdout instead of to a file")
}

// AddCoreFlags adds the core-executable, core-config, batch-size, and export{type} flags, which are needed for commands that use captive core
func AddCoreFlags(flags *pflag.FlagSet) {
	flags.StringP("core-executable", "x", "", "Filepath to the stellar-core executable")
	flags.StringP("core-config", "c", "", "Filepath to the a config file for stellar-core")

	flags.BoolP("export-accounts", "a", false, "set in order to export account changes")
	flags.BoolP("export-trustlines", "t", false, "set in order to export trustline changes")
	flags.BoolP("export-offers", "f", false, "set in order to export offer changes")

	flags.Uint32P("batch-size", "b", 64, "number of ledgers to export changes from in each batches")
}

// AddBucketFlags adds the end-ledger, output, and stdout flags, which are needed for commands that use the bucket list
func AddBucketFlags(objectName string, flags *pflag.FlagSet) {
	flags.Uint32P("end-ledger", "e", 0, "The ledger sequence number for the end of the export range (required)")
	flags.StringP("output", "o", "exported_"+objectName+".txt", "Filename of the output file")
	flags.Bool("stdout", false, "If set, the output will be printed to stdout instead of to a file")
}

// MustBasicFlags gets the values of the start-ledger, end-ledger, limit, output, and stdout flags from the flag set. If any do not exist, it stops the program fatally using the logger
func MustBasicFlags(flags *pflag.FlagSet, logger *log.Entry) (startNum, endNum uint32, limit int64, path string, useStdout bool) {
	startNum, err := flags.GetUint32("start-ledger")
	if err != nil {
		logger.Fatal("could not get start sequence number: ", err)
	}

	endNum, err = flags.GetUint32("end-ledger")
	if err != nil {
		logger.Fatal("could not get end sequence number: ", err)
	}

	limit, err = flags.GetInt64("limit")
	if err != nil {
		logger.Fatal("could not get limit: ", err)
	}

	path, err = flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	useStdout, err = flags.GetBool("stdout")
	if err != nil {
		logger.Fatal("could not get stdout boolean: ", err)
	}

	return
}

// MustCoreFlags gets the values for the core-executable, core-config, batch-size, and export{type} flags. If any do not exist, it stops the program fatally using the logger
func MustCoreFlags(flags *pflag.FlagSet, logger *log.Entry) (execPath, configPath string, exportAccounts, exportOffers, exportTrustlines bool, batchSize uint32) {
	execPath, err := flags.GetString("core-executable")
	if err != nil {
		logger.Fatal("could not get path to stellar-core executable, which is mandatory when not starting at the genesis ledger (ledger 1): ", err)
	}

	configPath, err = flags.GetString("core-config")
	if err != nil {
		logger.Fatal("could not get path to stellar-core config file, is mandatory when not starting at the genesis ledger (ledger 1): ", err)
	}

	exportAccounts, err = flags.GetBool("export-accounts")
	if err != nil {
		logger.Fatal("could not get export accounts flag: ", err)
	}

	exportOffers, err = flags.GetBool("export-offers")
	if err != nil {
		logger.Fatal("could not get export offers flag: ", err)
	}

	exportTrustlines, err = flags.GetBool("export-trustlines")
	if err != nil {
		logger.Fatal("could not get export trustlines flag: ", err)
	}

	batchSize, err = flags.GetUint32("batch-size")
	if err != nil {
		logger.Fatal("could not get batch size: ", err)
	}

	return
}

// MustBucketFlags gets the values of the end-ledger, output, and stdout flags from the flag set. If any do not exist, it stops the program fatally using the logger
func MustBucketFlags(flags *pflag.FlagSet, logger *log.Entry) (endNum uint32, path string, useStdout bool) {
	endNum, err := flags.GetUint32("end-ledger")
	if err != nil {
		logger.Fatal("could not get end sequence number: ", err)
	}

	path, err = flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	useStdout, err = flags.GetBool("stdout")
	if err != nil {
		logger.Fatal("could not get stdout boolean: ", err)
	}

	return
}

// CreateBackend creates a history archive backend
func CreateBackend() (*ledgerbackend.HistoryArchiveBackend, error) {
	archiveStellarURL := "http://history.stellar.org/prd/core-live/core_live_001"
	return ledgerbackend.NewHistoryArchiveBackendFromURL(archiveStellarURL)
}

// GetCheckpointNum gets the ledger sequence number of the checkpoint containing the provided ledger. If the checkpoint does not exist, an error is returned
func GetCheckpointNum(seq, maxSeq uint32) (uint32, error) {
	/*
		Checkpoints are made "every 64 ledgers", when LCL is one-less-than a multiple
		of 64. In other words, at LCL=63, 127, 191, 255, etc. or in other other words
		checkpoint K covers the inclusive ledger range [K*64, ((K+1)*64)-1], and each
		of those ranges should contain exactly 64 ledgers, with the exception of the
		first checkpoint, which has only 63 ledgers: there is no ledger 0.
	*/
	remainder := (seq + 1) % 64
	if remainder == 0 {
		return seq, nil
	}

	checkpoint := seq + 64 - remainder
	if checkpoint > maxSeq {
		return 0, fmt.Errorf("The checkpoint ledger %d is greater than the max ledger number %d", checkpoint, maxSeq)
	}

	return checkpoint, nil
}
