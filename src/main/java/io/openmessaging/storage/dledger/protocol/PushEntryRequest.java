package io.openmessaging.storage.dledger.protocol;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.utils.PreConditions;

import java.util.ArrayList;
import java.util.List;

/**
 * 同步请求
 */
public class PushEntryRequest extends RequestOrResponse {

    /**
     * 同步时顺带提交位置
     */
    private long commitIndex = -1;

    /**
     * 追加类型
     */
    private Type type = Type.APPEND;

    /**
     * 单体同步时单体日志
     */
    private DLedgerEntry entry;


    /**
     * 批量同步时批量日志
     */
    private final List<DLedgerEntry> batchEntry = new ArrayList<>();

    /**
     * 批量同步时批量日志总数
     */
    private int totalSize;

    public DLedgerEntry getEntry() {
        return entry;
    }

    public void setEntry(DLedgerEntry entry) {
        this.entry = entry;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public void addEntry(DLedgerEntry entry) {
        if (!batchEntry.isEmpty()) {
            PreConditions.check(batchEntry.get(0).getIndex() + batchEntry.size() == entry.getIndex(),
                    DLedgerResponseCode.UNKNOWN, "batch push wrong order");
        }
        batchEntry.add(entry);
        totalSize += entry.getSize();
    }

    public long getFirstEntryIndex() {
        if (!batchEntry.isEmpty()) {
            return batchEntry.get(0).getIndex();
        } else if (entry != null) {
            return entry.getIndex();
        } else {
            return -1;
        }
    }

    public long getLastEntryIndex() {
        if (!batchEntry.isEmpty()) {
            return batchEntry.get(batchEntry.size() - 1).getIndex();
        } else if (entry != null) {
            return entry.getIndex();
        } else {
            return -1;
        }
    }

    public int getCount() {
        return batchEntry.size();
    }

    public long getTotalSize() {
        return totalSize;
    }

    public List<DLedgerEntry> getBatchEntry() {
        return batchEntry;
    }

    public void clear() {
        batchEntry.clear();
        totalSize = 0;
    }

    public boolean isBatch() {
        return !batchEntry.isEmpty();
    }

    public enum Type {
        APPEND,
        COMMIT,
        COMPARE,
        TRUNCATE
    }
}
