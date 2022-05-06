package io.openmessaging.storage.dledger.protocol;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

/**
 * 提案查询结果
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class GetEntriesResponse extends RequestOrResponse {

    /**
     * 所有查询的提案
     */
    private List<DLedgerEntry> entries = new ArrayList<>();

}
