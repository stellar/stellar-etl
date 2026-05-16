package cdptest

import (
	"io"
	"maps"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/cdptest"
)

var ledgerFunctionMap = map[string]interface{}{
	"LedgerSequence":            cdptest.LedgerSequence,
	"CloseTime":                 cdptest.CloseTime,
	"BaseFee":                   cdptest.BaseFee,
	"BaseReserve":               cdptest.BaseReserve,
	"SorobanFeeWrite1Kb":        cdptest.SorobanFeeWrite1Kb,
	"TotalByteSizeOfBucketList": cdptest.TotalByteSizeOfBucketList,
}

var transactionFunctionMap = map[string]interface{}{
	"TransactionHash":     cdptest.TransactionHash,
	"Account":             cdptest.Account,
	"TransactionEnvelope": cdptest.TransactionEnvelope,
}

func GenericProcessor(lcm xdr.LedgerCloseMeta, processorJSON string) ([]map[string]interface{}, error) {
	// result can be any number of key:val
	var ledgerResult map[string]interface{}

	// Get the functions from the *processor.json file
	funcs := make(map[string][]string)

	// LedgerFunctions are single value
	for _, funcName := range funcs["functions"] {
		fn, ok := ledgerFunctionMap[funcName]
		if ok {
			f := fn.(func(xdr.LedgerCloseMeta) interface{})
			ledgerResult[funcName] = f(lcm)
		}
	}

	hasTx := false
	for _, key := range funcs["functions"] {
		_, ok := transactionFunctionMap[key]
		if ok {
			hasTx = true
		}
	}

	if !hasTx {
		var results []map[string]interface{}
		results = append(results, ledgerResult)
		return results, nil
	}

	// There can be multiple tx in LCM
	var transactionResult []map[string]interface{}

	txReader, _ := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta("password", lcm)
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}

		var result map[string]interface{}
		for _, funcName := range funcs["functions"] {

			fn, ok := transactionFunctionMap[funcName]
			if ok {
				f := fn.(func(ingest.LedgerTransaction) interface{})
				result[funcName] = f(tx)
			}
		}
		maps.Copy(result, ledgerResult)
		transactionResult = append(transactionResult, result)
	}

	return transactionResult, nil
}
