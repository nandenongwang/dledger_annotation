package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.utils.IOUtils;
import io.openmessaging.storage.dledger.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static io.openmessaging.storage.dledger.MemberState.Role.*;

/**
 * 节点状态
 */
public class MemberState {

    public static final String TERM_PERSIST_FILE = "currterm";
    public static final String TERM_PERSIST_KEY_TERM = "currTerm";
    public static final String TERM_PERSIST_KEY_VOTE_FOR = "voteLeader";
    public static Logger logger = LoggerFactory.getLogger(MemberState.class);
    public final DLedgerConfig dLedgerConfig;
    private final ReentrantLock defaultLock = new ReentrantLock();

    /**
     * dledger集群分组
     */
    private final String group;

    /**
     * 节点ID
     */
    private final String selfId;

    /**
     * 所有节点ID&地址 【id1-addr1;id2-adr2;id3-addr3】
     */
    private final String peers;

    /**
     * 当前节点角色
     */
    private volatile Role role = CANDIDATE;

    /**
     * 当前leader
     */
    private volatile String leaderId;

    /**
     * 当前朝代
     */
    private volatile long currTerm = 0;

    /**
     * 当前已投票节点
     */
    private volatile String currVoteFor;

    /**
     * 最新提案序号
     */
    private volatile long ledgerEndIndex = -1;

    /**
     * 最新提案任期
     */
    private volatile long ledgerEndTerm = -1;

    /**
     * 连接节点中最大任期
     */
    private long knownMaxTermInGroup = -1;

    /**
     * 所有配置peers节点
     */
    private final Map<String/*节点ID */, String/* 地址 */> peerMap = new HashMap<>();

    /**
     * 节点连接状态
     */
    private Map<String/* 节点ID */, Boolean/* 网络是否正常 */> peersLiveTable = new ConcurrentHashMap<>();

    /**
     * leader待转移leader的followerId 【leader节点等待新leader转移时设置为新leaderId】
     */
    private volatile String transferee;

    /**
     * follower待成为leader的新任期
     */
    private volatile long termToTakeLeadership = -1;

    public MemberState(DLedgerConfig config) {
        this.group = config.getGroup();
        this.selfId = config.getSelfId();
        this.peers = config.getPeers();
        for (String peerInfo : this.peers.split(";")) {
            String peerSelfId = peerInfo.split("-")[0];
            String peerAddress = peerInfo.substring(peerSelfId.length() + 1);
            peerMap.put(peerSelfId, peerAddress);
        }
        this.dLedgerConfig = config;
        loadTerm();
    }

    /**
     * 从currterm文件中恢复当前朝代及上次投票节点ID 【默认0、null】
     */
    private void loadTerm() {
        try {
            String data = IOUtils.file2String(dLedgerConfig.getDefaultPath() + File.separator + TERM_PERSIST_FILE);
            Properties properties = IOUtils.string2Properties(data);
            if (properties == null) {
                return;
            }
            if (properties.containsKey(TERM_PERSIST_KEY_TERM)) {
                currTerm = Long.valueOf(String.valueOf(properties.get(TERM_PERSIST_KEY_TERM)));
            }
            if (properties.containsKey(TERM_PERSIST_KEY_VOTE_FOR)) {
                currVoteFor = String.valueOf(properties.get(TERM_PERSIST_KEY_VOTE_FOR));
                if (currVoteFor.length() == 0) {
                    currVoteFor = null;
                }
            }
        } catch (Throwable t) {
            logger.error("Load last term failed", t);
        }
    }

    /**
     * 将当前朝代及投票节点ID持久化到currterm文件中
     */
    private void persistTerm() {
        try {
            Properties properties = new Properties();
            properties.put(TERM_PERSIST_KEY_TERM, currTerm);
            properties.put(TERM_PERSIST_KEY_VOTE_FOR, currVoteFor == null ? "" : currVoteFor);
            String data = IOUtils.properties2String(properties);
            IOUtils.string2File(data, dLedgerConfig.getDefaultPath() + File.separator + TERM_PERSIST_FILE);
        } catch (Throwable t) {
            logger.error("Persist curr term failed", t);
        }
    }

    /**
     * 获取所处朝代
     */
    public long currTerm() {
        return currTerm;
    }

    /**
     * 获取投票节点ID
     */
    public String currVoteFor() {
        return currVoteFor;
    }

    /**
     * 设置投票节点ID并持久化
     */
    public synchronized void setCurrVoteFor(String currVoteFor) {
        this.currVoteFor = currVoteFor;
        persistTerm();
    }

    /**
     * 竞选任期 【最大任期或递增当前任期】
     */
    public synchronized long nextTerm() {
        //检查操作节点是否是master
        PreConditions.check(role == CANDIDATE, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "%s != %s", role, CANDIDATE);
        if (knownMaxTermInGroup > currTerm) {
            currTerm = knownMaxTermInGroup;
        } else {
            ++currTerm;
        }
        currVoteFor = null;
        persistTerm();
        return currTerm;
    }

    /**
     * 变更节点角色到leader
     */
    public synchronized void changeToLeader(long term) {
        PreConditions.check(currTerm == term, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "%d != %d", currTerm, term);
        this.role = LEADER;
        this.leaderId = selfId;
        peersLiveTable.clear();
    }

    /**
     * 变更节点角色到follower
     */
    public synchronized void changeToFollower(long term, String leaderId) {
        PreConditions.check(currTerm == term, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "%d != %d", currTerm, term);
        this.role = FOLLOWER;
        this.leaderId = leaderId;
        transferee = null;
    }

    /**
     * 变更节点角色到candidate
     */
    public synchronized void changeToCandidate(long term) {
        assert term >= currTerm;
        PreConditions.check(term >= currTerm, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "should %d >= %d", term, currTerm);
        if (term > knownMaxTermInGroup) {
            knownMaxTermInGroup = term;
        }
        //the currTerm should be promoted in handleVote thread
        this.role = CANDIDATE;
        this.leaderId = null;
        transferee = null;
    }

    public String getTransferee() {
        return transferee;
    }

    public void setTransferee(String transferee) {
        PreConditions.check(role == LEADER, DLedgerResponseCode.ILLEGAL_MEMBER_STATE, "%s is not leader", selfId);
        this.transferee = transferee;
    }

    public long getTermToTakeLeadership() {
        return termToTakeLeadership;
    }

    public void setTermToTakeLeadership(long termToTakeLeadership) {
        this.termToTakeLeadership = termToTakeLeadership;
    }

    public String getSelfId() {
        return selfId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public String getGroup() {
        return group;
    }

    public String getSelfAddr() {
        return peerMap.get(selfId);
    }

    public String getLeaderAddr() {
        return peerMap.get(leaderId);
    }

    public String getPeerAddr(String peerId) {
        return peerMap.get(peerId);
    }

    public boolean isLeader() {
        return role == LEADER;
    }

    public boolean isFollower() {
        return role == FOLLOWER;
    }

    public boolean isCandidate() {
        return role == CANDIDATE;
    }

    public boolean isQuorum(int num) {
        return num >= ((peerSize() / 2) + 1);
    }

    public int peerSize() {
        return peerMap.size();
    }

    public boolean isPeerMember(String id) {
        return id != null && peerMap.containsKey(id);
    }

    public Map<String, String> getPeerMap() {
        return peerMap;
    }

    public Map<String, Boolean> getPeersLiveTable() {
        return peersLiveTable;
    }

    //just for test
    public void setCurrTermForTest(long term) {
        PreConditions.check(term >= currTerm, DLedgerResponseCode.ILLEGAL_MEMBER_STATE);
        this.currTerm = term;
    }

    public Role getRole() {
        return role;
    }

    public ReentrantLock getDefaultLock() {
        return defaultLock;
    }

    public void updateLedgerIndexAndTerm(long index, long term) {
        this.ledgerEndIndex = index;
        this.ledgerEndTerm = term;
    }

    public long getLedgerEndIndex() {
        return ledgerEndIndex;
    }

    public long getLedgerEndTerm() {
        return ledgerEndTerm;
    }

    /**
     * 节点角色 【主、从、候选者】
     */
    public enum Role {
        UNKNOWN,
        CANDIDATE,
        LEADER,
        FOLLOWER;
    }
}
