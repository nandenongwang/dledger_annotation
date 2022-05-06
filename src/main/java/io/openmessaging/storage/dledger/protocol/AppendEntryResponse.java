package io.openmessaging.storage.dledger.protocol;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 增加提案结果
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class AppendEntryResponse extends RequestOrResponse {

    /**
     * 提案序号
     */
    private long index = -1;

    /**
     *
     */
    private long pos = -1;
}
