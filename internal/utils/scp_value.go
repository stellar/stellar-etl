package utils

import (
	"encoding/base64"
	"fmt"
	"math"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ScpValueInfo holds everything stellar-etl reads out of StellarValue.ext, the
// union CAP-88 grew from three arms to five. Between them the arms carry three
// values we want and no caller wants only one of them, so they are read once
// and returned together.
type ScpValueInfo struct {
	// NodeID is the validator that nominated the ledger. Empty on
	// STELLAR_VALUE_BASIC, which carries no signature by design.
	NodeID string
	// Signature is the base64 close value signature. Empty whenever NodeID is.
	Signature string
	// CloseTimeMs is the ledger close time in milliseconds. Only the _MS arms
	// carry real sub-second precision; the older arms carry whole seconds and
	// are scaled by 1000, so the value is populated across all of history and
	// every pre-Protocol-30 value ends in 000.
	CloseTimeMs int64
}

// ScpValueInfoFromHeader reads StellarValue.ext by switching on its
// discriminant.
//
// It deliberately avoids the generated Get<Arm>() accessors. Those compare the
// arm *name* against a string literal and return ok=false on any other arm, so
// a caller that ignores ok keeps a zero value and writes an empty column with
// no error. That is precisely how CAP-88 emptied history_ledgers.node_id:
// lcValueSignature stopped being the whole body of the arm in use and became a
// field inside signedMsValue. Switching on V makes the next new arm a loud
// failure here rather than a silent empty value downstream.
//
// This is the only function in the repo that should name an arm of this union.
func ScpValueInfoFromHeader(lhe xdr.LedgerHeaderHistoryEntry) (ScpValueInfo, error) {
	scpValue := lhe.Header.ScpValue

	closeTime := int64(scpValue.CloseTime)
	if closeTime < 0 || closeTime > math.MaxInt64/1000 {
		return ScpValueInfo{}, fmt.Errorf("close time out of range: %d", scpValue.CloseTime)
	}

	// Whole seconds unless an _MS arm overrides it below.
	info := ScpValueInfo{CloseTimeMs: closeTime * 1000}

	var signature xdr.LedgerCloseValueSignature
	switch scpValue.Ext.V {
	case xdr.StellarValueTypeStellarValueBasic:
		// Legacy arm. It carries no signature, so empty NodeID and Signature
		// are the correct output rather than an error. Roughly half of pubnet
		// history is this arm and any backfill runs over it.
		return info, nil

	case xdr.StellarValueTypeStellarValueSigned:
		if scpValue.Ext.LcValueSignature == nil {
			return ScpValueInfo{}, fmt.Errorf("STELLAR_VALUE_SIGNED with no lcValueSignature")
		}
		signature = *scpValue.Ext.LcValueSignature

	case xdr.StellarValueTypeStellarValueEmptyTxSet:
		if scpValue.Ext.ProposedValue == nil {
			return ScpValueInfo{}, fmt.Errorf("STELLAR_VALUE_EMPTY_TX_SET with no proposedValue")
		}
		signature = scpValue.Ext.ProposedValue.LcValueSignature

	case xdr.StellarValueTypeStellarValueSignedMs:
		if scpValue.Ext.SignedMsValue == nil {
			return ScpValueInfo{}, fmt.Errorf("STELLAR_VALUE_SIGNED_MS with no signedMsValue")
		}
		info.CloseTimeMs = int64(scpValue.Ext.SignedMsValue.CloseTimeMs)
		signature = scpValue.Ext.SignedMsValue.LcValueSignature

	case xdr.StellarValueTypeStellarValueEmptyTxSetMs:
		if scpValue.Ext.ProposedMsValue == nil {
			return ScpValueInfo{}, fmt.Errorf("STELLAR_VALUE_EMPTY_TX_SET_MS with no proposedMsValue")
		}
		info.CloseTimeMs = int64(scpValue.Ext.ProposedMsValue.CloseTimeMs)
		signature = scpValue.Ext.ProposedMsValue.LcValueSignature

	default:
		return ScpValueInfo{}, fmt.Errorf("unknown StellarValue.ext arm: %d", scpValue.Ext.V)
	}

	nodeID, err := getNodeAddress(signature.NodeId)
	if err != nil {
		return ScpValueInfo{}, err
	}

	info.NodeID = nodeID
	info.Signature = base64.StdEncoding.EncodeToString(signature.Signature)
	return info, nil
}

// ScpValueInfoFromLedgerCloseMeta is ScpValueInfoFromHeader for callers holding
// the whole ledger rather than just its header.
func ScpValueInfoFromLedgerCloseMeta(lcm xdr.LedgerCloseMeta) (ScpValueInfo, error) {
	return ScpValueInfoFromHeader(lcm.LedgerHeaderHistoryEntry())
}

// getNodeAddress encodes a validator node id as a strkey account address.
func getNodeAddress(nodeID xdr.NodeId) (string, error) {
	switch nodeID.Type {
	case xdr.PublicKeyTypePublicKeyTypeEd25519:
		// PublicKeyTypeEd25519 is 0, so a zero-valued NodeId selects this arm
		// with a nil key, and the generated GetEd25519() dereferences it
		// without checking. Fail with an error rather than panicking the
		// whole export.
		if nodeID.Ed25519 == nil {
			return "", fmt.Errorf("node id is typed Ed25519 but carries no key")
		}
		ed, ok := nodeID.GetEd25519()
		if !ok {
			return "", fmt.Errorf("could not get Ed25519")
		}
		raw := make([]byte, 32)
		copy(raw, ed[:])
		return strkey.Encode(strkey.VersionByteAccountID, raw)
	default:
		return "", fmt.Errorf("unknown node id type: %v", nodeID.Type)
	}
}
