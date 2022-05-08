package io.openmessaging.storage.dledger.protocol;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * leader手动转移请求
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class LeadershipTransferRequest extends RequestOrResponse {


    /**
     * 当前leaderId
     */
    private String transferId;

    /**
     * 指定新leaderId
     */
    private String transfereeId;

    /**
     *
     */
    private long takeLeadershipLedgerIndex;
}
