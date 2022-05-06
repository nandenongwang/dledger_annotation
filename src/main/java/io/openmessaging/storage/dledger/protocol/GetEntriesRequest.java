package io.openmessaging.storage.dledger.protocol;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class GetEntriesRequest extends RequestOrResponse {
    private Long beginIndex;

    private int maxSize;

    private List<Long> indexList;
}
