package io.openmessaging.storage.dledger.protocol;

/**
 * 手动转移leader结果
 */
public class LeadershipTransferResponse extends RequestOrResponse {

    public LeadershipTransferResponse term(long term) {
        this.term = term;
        return this;
    }

    @Override
    public LeadershipTransferResponse code(int code) {
        this.code = code;
        return this;
    }

    @Override
    public String toString() {
        return "LeadershipTransferResponse{" +
                "group='" + group + '\'' +
                ", remoteId='" + remoteId + '\'' +
                ", localId='" + localId + '\'' +
                ", code=" + code +
                ", leaderId='" + leaderId + '\'' +
                ", term=" + term +
                '}';
    }
}
