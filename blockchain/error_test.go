// Copyright (c) 2014 The btcsuite developers
// Copyright (c) 2015-2018 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"errors"
	"io"
	"testing"
)

// TestErrorKindStringer tests the stringized output for the ErrorKind type.
func TestErrorCodeStringer(t *testing.T) {
	tests := []struct {
		in   ErrorKind
		want string
	}{
		{ErrDuplicateBlock, "ErrDuplicateBlock"},
		{ErrMissingParent, "ErrMissingParent"},
		{ErrBlockTooBig, "ErrBlockTooBig"},
		{ErrWrongBlockSize, "ErrWrongBlockSize"},
		{ErrBlockVersionTooOld, "ErrBlockVersionTooOld"},
		{ErrBadStakeVersion, "ErrBadStakeVersion"},
		{ErrInvalidTime, "ErrInvalidTime"},
		{ErrTimeTooOld, "ErrTimeTooOld"},
		{ErrTimeTooNew, "ErrTimeTooNew"},
		{ErrDifficultyTooLow, "ErrDifficultyTooLow"},
		{ErrUnexpectedDifficulty, "ErrUnexpectedDifficulty"},
		{ErrHighHash, "ErrHighHash"},
		{ErrBadMerkleRoot, "ErrBadMerkleRoot"},
		{ErrBadCommitmentRoot, "ErrBadCommitmentRoot"},
		{ErrBadCheckpoint, "ErrBadCheckpoint"},
		{ErrForkTooOld, "ErrForkTooOld"},
		{ErrCheckpointTimeTooOld, "ErrCheckpointTimeTooOld"},
		{ErrNoTransactions, "ErrNoTransactions"},
		{ErrTooManyTransactions, "ErrTooManyTransactions"},
		{ErrNoTxInputs, "ErrNoTxInputs"},
		{ErrNoTxOutputs, "ErrNoTxOutputs"},
		{ErrTxTooBig, "ErrTxTooBig"},
		{ErrBadTxOutValue, "ErrBadTxOutValue"},
		{ErrDuplicateTxInputs, "ErrDuplicateTxInputs"},
		{ErrBadTxInput, "ErrBadTxInput"},
		{ErrMissingTxOut, "ErrMissingTxOut"},
		{ErrUnfinalizedTx, "ErrUnfinalizedTx"},
		{ErrDuplicateTx, "ErrDuplicateTx"},
		{ErrOverwriteTx, "ErrOverwriteTx"},
		{ErrImmatureSpend, "ErrImmatureSpend"},
		{ErrSpendTooHigh, "ErrSpendTooHigh"},
		{ErrBadFees, "ErrBadFees"},
		{ErrTooManySigOps, "ErrTooManySigOps"},
		{ErrFirstTxNotCoinbase, "ErrFirstTxNotCoinbase"},
		{ErrCoinbaseHeight, "ErrCoinbaseHeight"},
		{ErrMultipleCoinbases, "ErrMultipleCoinbases"},
		{ErrStakeTxInRegularTree, "ErrStakeTxInRegularTree"},
		{ErrRegTxInStakeTree, "ErrRegTxInStakeTree"},
		{ErrBadCoinbaseScriptLen, "ErrBadCoinbaseScriptLen"},
		{ErrBadCoinbaseValue, "ErrBadCoinbaseValue"},
		{ErrBadCoinbaseOutpoint, "ErrBadCoinbaseOutpoint"},
		{ErrBadCoinbaseFraudProof, "ErrBadCoinbaseFraudProof"},
		{ErrBadCoinbaseAmountIn, "ErrBadCoinbaseAmountIn"},
		{ErrBadStakebaseAmountIn, "ErrBadStakebaseAmountIn"},
		{ErrBadStakebaseScriptLen, "ErrBadStakebaseScriptLen"},
		{ErrBadStakebaseScrVal, "ErrBadStakebaseScrVal"},
		{ErrScriptMalformed, "ErrScriptMalformed"},
		{ErrScriptValidation, "ErrScriptValidation"},
		{ErrNotEnoughStake, "ErrNotEnoughStake"},
		{ErrStakeBelowMinimum, "ErrStakeBelowMinimum"},
		{ErrNonstandardStakeTx, "ErrNonstandardStakeTx"},
		{ErrNotEnoughVotes, "ErrNotEnoughVotes"},
		{ErrTooManyVotes, "ErrTooManyVotes"},
		{ErrFreshStakeMismatch, "ErrFreshStakeMismatch"},
		{ErrTooManySStxs, "ErrTooManySStxs"},
		{ErrInvalidEarlyStakeTx, "ErrInvalidEarlyStakeTx"},
		{ErrTicketUnavailable, "ErrTicketUnavailable"},
		{ErrVotesOnWrongBlock, "ErrVotesOnWrongBlock"},
		{ErrVotesMismatch, "ErrVotesMismatch"},
		{ErrIncongruentVotebit, "ErrIncongruentVotebit"},
		{ErrInvalidSSRtx, "ErrInvalidSSRtx"},
		{ErrRevocationsMismatch, "ErrRevocationsMismatch"},
		{ErrTooManyRevocations, "ErrTooManyRevocations"},
		{ErrTicketCommitment, "ErrTicketCommitment"},
		{ErrInvalidVoteInput, "ErrInvalidVoteInput"},
		{ErrBadNumPayees, "ErrBadNumPayees"},
		{ErrBadPayeeScriptVersion, "ErrBadPayeeScriptVersion"},
		{ErrBadPayeeScriptType, "ErrBadPayeeScriptType"},
		{ErrMismatchedPayeeHash, "ErrMismatchedPayeeHash"},
		{ErrBadPayeeValue, "ErrBadPayeeValue"},
		{ErrSSGenSubsidy, "ErrSSGenSubsidy"},
		{ErrImmatureTicketSpend, "ErrImmatureTicketSpend"},
		{ErrTicketInputScript, "ErrTicketInputScript"},
		{ErrInvalidRevokeInput, "ErrInvalidRevokeInput"},
		{ErrSSRtxPayees, "ErrSSRtxPayees"},
		{ErrTxSStxOutSpend, "ErrTxSStxOutSpend"},
		{ErrRegTxCreateStakeOut, "ErrRegTxCreateStakeOut"},
		{ErrInvalidFinalState, "ErrInvalidFinalState"},
		{ErrPoolSize, "ErrPoolSize"},
		{ErrForceReorgWrongChain, "ErrForceReorgWrongChain"},
		{ErrForceReorgMissingChild, "ErrForceReorgMissingChild"},
		{ErrBadStakebaseValue, "ErrBadStakebaseValue"},
		{ErrDiscordantTxTree, "ErrDiscordantTxTree"},
		{ErrStakeFees, "ErrStakeFees"},
		{ErrNoStakeTx, "ErrNoStakeTx"},
		{ErrBadBlockHeight, "ErrBadBlockHeight"},
		{ErrBlockOneTx, "ErrBlockOneTx"},
		{ErrBlockOneInputs, "ErrBlockOneInputs"},
		{ErrBlockOneOutputs, "ErrBlockOneOutputs"},
		{ErrNoTax, "ErrNoTax"},
		{ErrExpiredTx, "ErrExpiredTx"},
		{ErrExpiryTxSpentEarly, "ErrExpiryTxSpentEarly"},
		{ErrFraudAmountIn, "ErrFraudAmountIn"},
		{ErrFraudBlockHeight, "ErrFraudBlockHeight"},
		{ErrFraudBlockIndex, "ErrFraudBlockIndex"},
		{ErrZeroValueOutputSpend, "ErrZeroValueOutputSpend"},
		{ErrInvalidEarlyVoteBits, "ErrInvalidEarlyVoteBits"},
		{ErrInvalidEarlyFinalState, "ErrInvalidEarlyFinalState"},
		{ErrKnownInvalidBlock, "ErrKnownInvalidBlock"},
		{ErrInvalidAncestorBlock, "ErrInvalidAncestorBlock"},
		{ErrInvalidTemplateParent, "ErrInvalidTemplateParent"},
	}

	t.Logf("Running %d tests", len(tests))
	for i, test := range tests {
		result := test.in.Error()
		if result != test.want {
			t.Errorf("String #%d\n got: %s want: %s", i, result,
				test.want)
			continue
		}
	}
}

// TestRuleError tests the error output for the RuleError type.
func TestRuleError(t *testing.T) {
	tests := []struct {
		in   RuleError
		want string
	}{
		{
			RuleError{Description: "duplicate block"},
			"duplicate block",
		},
		{
			RuleError{Description: "human-readable error"},
			"human-readable error",
		},
	}

	t.Logf("Running %d tests", len(tests))
	for i, test := range tests {
		result := test.in.Error()
		if result != test.want {
			t.Errorf("Error #%d\n got: %s want: %s", i, result,
				test.want)
			continue
		}
	}
}

// TestErrorKindIsAs ensures both ErrorKind and Error can be identified as being
// a specific error kind via errors.Is and unwrapped via errors.As.
func TestErrorKindIsAs(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		target    error
		wantMatch bool
		wantAs    ErrorKind
	}{{
		name:      "ErrDuplicateBlock == ErrDuplicateBlock",
		err:       ErrDuplicateBlock,
		target:    ErrDuplicateBlock,
		wantMatch: true,
		wantAs:    ErrDuplicateBlock,
	}, {
		name:      "RuleError.ErrDuplicateBlock == ErrDuplicateBlock",
		err:       ruleError(ErrDuplicateBlock, ""),
		target:    ErrDuplicateBlock,
		wantMatch: true,
		wantAs:    ErrDuplicateBlock,
	}, {
		name:      "RuleError.ErrDuplicateBlock == RuleError.ErrDuplicateBlock",
		err:       ruleError(ErrDuplicateBlock, ""),
		target:    ruleError(ErrDuplicateBlock, ""),
		wantMatch: true,
		wantAs:    ErrDuplicateBlock,
	}, {
		name:      "ErrDuplicateBlock != ErrMissingParent",
		err:       ErrDuplicateBlock,
		target:    ErrMissingParent,
		wantMatch: false,
		wantAs:    ErrDuplicateBlock,
	}, {
		name:      "RuleError.ErrDuplicateBlock != ErrMissingParent",
		err:       ruleError(ErrDuplicateBlock, ""),
		target:    ErrMissingParent,
		wantMatch: false,
		wantAs:    ErrDuplicateBlock,
	}, {
		name:      "ErrDuplicateBlock != RuleError.ErrMissingParent",
		err:       ErrDuplicateBlock,
		target:    ruleError(ErrMissingParent, ""),
		wantMatch: false,
		wantAs:    ErrDuplicateBlock,
	}, {
		name:      "RuleError.ErrDuplicateBlock != RuleError.ErrMissingParent",
		err:       ruleError(ErrDuplicateBlock, ""),
		target:    ruleError(ErrMissingParent, ""),
		wantMatch: false,
		wantAs:    ErrDuplicateBlock,
	}, {
		name:      "RuleError.ErrDuplicateBlock != io.EOF",
		err:       ruleError(ErrDuplicateBlock, ""),
		target:    io.EOF,
		wantMatch: false,
		wantAs:    ErrDuplicateBlock,
	}}

	for _, test := range tests {
		// Ensure the error matches or not depending on the expected result.
		result := errors.Is(test.err, test.target)
		if result != test.wantMatch {
			t.Errorf("%s: incorrect error identification -- got %v, want %v",
				test.name, result, test.wantMatch)
			continue
		}

		// Ensure the underlying error kind can be unwrapped is and is the
		// expected kind.
		var kind ErrorKind
		if !errors.As(test.err, &kind) {
			t.Errorf("%s: unable to unwrap to error kind", test.name)
			continue
		}
		if kind != test.wantAs {
			t.Errorf("%s: unexpected unwrapped error kind -- got %v, want %v",
				test.name, kind, test.wantAs)
			continue
		}
	}
}
