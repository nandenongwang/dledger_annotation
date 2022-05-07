package io.openmessaging.storage.dledger.store;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.utils.PreConditions;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 提案内存存储服务
 */
public class DLedgerMemoryStore extends DLedgerStore {

    private static final Logger logger = LoggerFactory.getLogger(DLedgerMemoryStore.class);

    /**
     * 存储的第一条提案序号
     */
    @Getter
    private long ledgerBeginIndex = -1;

    /**
     * 存储的最后一条提案序号
     */
    @Getter
    private long ledgerEndIndex = -1;

    /**
     * 确认的提案序号 【内存存储默认与ledgerEndIndex相同】
     */
    @Getter
    private long committedIndex = -1;

    /**
     * 最后一条提案任期号
     */
    @Getter
    private long ledgerEndTerm;

    /**
     * 内存提案存储
     */
    private final Map<Long, DLedgerEntry> cachedEntries = new ConcurrentHashMap<>();

    /**
     * dledger配置
     */
    private final DLedgerConfig dLedgerConfig;

    /**
     * 节点状态
     */
    private final MemberState memberState;

    public DLedgerMemoryStore(DLedgerConfig dLedgerConfig, MemberState memberState) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
    }

    /**
     * leader存储提案条目 【存储提案、最后序号&确认序号递增】
     */
    @Override
    public DLedgerEntry appendAsLeader(DLedgerEntry entry) {
        PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
        synchronized (memberState) {
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            PreConditions.check(memberState.getTransferee() == null, DLedgerResponseCode.LEADER_TRANSFERRING);
            ledgerEndIndex++;
            committedIndex++;
            ledgerEndTerm = memberState.currTerm();
            entry.setIndex(ledgerEndIndex);
            entry.setTerm(memberState.currTerm());
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Append as Leader {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            cachedEntries.put(entry.getIndex(), entry);
            if (ledgerBeginIndex == -1) {
                ledgerBeginIndex = ledgerEndIndex;
            }
            //更新节点状态中的最新日志索引和任期
            updateLedgerEndIndexAndTerm();
            return entry;
        }
    }

    /**
     * 丢弃指定提案位置之后的提案 【设置提案指针、之后的会被覆盖】
     */
    @Override
    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        return appendAsFollower(entry, leaderTerm, leaderId).getIndex();
    }

    /**
     * follower存储提案条目
     */
    @Override
    public DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER);
        synchronized (memberState) {
            PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER);
            PreConditions.check(leaderTerm == memberState.currTerm(), DLedgerResponseCode.INCONSISTENT_TERM);
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER);
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Append as Follower {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            ledgerEndTerm = entry.getTerm();
            ledgerEndIndex = entry.getIndex();
            committedIndex = entry.getIndex();
            cachedEntries.put(entry.getIndex(), entry);
            if (ledgerBeginIndex == -1) {
                ledgerBeginIndex = ledgerEndIndex;
            }
            //更新节点状态中的最新日志索引和任期
            updateLedgerEndIndexAndTerm();
            return entry;
        }

    }

    /**
     * 获取指定索引位置提案
     */
    @Override
    public DLedgerEntry get(Long index) {
        return cachedEntries.get(index);
    }
}
