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
     *
     */
    @Getter
    private long ledgerBeginIndex = -1;

    /**
     *
     */
    @Getter
    private long ledgerEndIndex = -1;

    /**
     *
     */
    @Getter
    private long committedIndex = -1;

    /**
     *
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
     *
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
     *
     */
    @Override
    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        return appendAsFollower(entry, leaderTerm, leaderId).getIndex();
    }

    /**
     *
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
