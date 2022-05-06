package io.openmessaging.storage.dledger.protocol;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * 元数据查询响应
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MetadataResponse extends RequestOrResponse {

    /**
     * 所有集群节点
     */
    private Map<String/* id */, String/* addr */> peers;

}
