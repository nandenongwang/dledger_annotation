package io.openmessaging.storage.dledger.protocol;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 投票请求参数
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class VoteRequest extends RequestOrResponse {

    /**
     *
     */
    private long ledgerEndIndex = -1;

    /**
     *请求投票节点
     */
    private long ledgerEndTerm = -1;
}
