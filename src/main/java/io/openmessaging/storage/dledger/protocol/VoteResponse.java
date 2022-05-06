package io.openmessaging.storage.dledger.protocol;

import static io.openmessaging.storage.dledger.protocol.VoteResponse.RESULT.UNKNOWN;

public class VoteResponse extends RequestOrResponse {

    public RESULT voteResult = UNKNOWN;

    public VoteResponse() {

    }

    public VoteResponse(VoteRequest request) {
        copyBaseInfo(request);
    }

    public RESULT getVoteResult() {
        return voteResult;
    }

    public void setVoteResult(RESULT voteResult) {
        this.voteResult = voteResult;
    }

    public VoteResponse voteResult(RESULT voteResult) {
        this.voteResult = voteResult;
        return this;
    }

    public VoteResponse term(long term) {
        this.term = term;
        return this;
    }

    public enum RESULT {
        UNKNOWN,
        ACCEPT,
        REJECT_UNKNOWN_LEADER,
        REJECT_UNEXPECTED_LEADER,
        REJECT_EXPIRED_VOTE_TERM,
        REJECT_ALREADY_VOTED,
        REJECT_ALREADY_HAS_LEADER,
        REJECT_TERM_NOT_READY,
        REJECT_TERM_SMALL_THAN_LEDGER,
        REJECT_EXPIRED_LEDGER_TERM,
        REJECT_SMALL_LEDGER_END_INDEX,
        REJECT_TAKING_LEADERSHIP;
    }

    /**
     * 投票结果
     */
    public enum ParseResult {

        /**
         *
         */
        WAIT_TO_REVOTE,

        /**
         *
         */
        REVOTE_IMMEDIATELY,

        /**
         * 通过 【获取了多数节点投票、成为leader】
         */
        PASSED,

        /**
         *
         */
        WAIT_TO_VOTE_NEXT;
    }
}
