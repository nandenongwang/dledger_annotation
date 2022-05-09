package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.protocol.*;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 选举服务
 */
public class DLedgerLeaderElector {

    private static final Logger logger = LoggerFactory.getLogger(DLedgerLeaderElector.class);

    private final Random random = new Random();

    /**
     * dledger配置
     */
    private final DLedgerConfig dLedgerConfig;

    /**
     * 节点状态
     */
    private final MemberState memberState;

    /**
     * RPC接口实现
     */
    private final DLedgerRpcService dLedgerRpcService;

    //as a server handler
    //record the last leader state
    private volatile long lastLeaderHeartBeatTime = -1;
    private volatile long lastSendHeartBeatTime = -1;

    /**
     * 上次成功维护心跳时间 【leader到followers、candidates】
     */
    private volatile long lastSuccHeartBeatTime = -1;

    private int heartBeatTimeIntervalMs = 2000;
    private int maxHeartBeatLeak = 3;

    //as a client
    /**
     * candidate下次发送投票请求时间
     */
    private long nextTimeToRequestVote = -1;

    /**
     * 是否需要递增竞选任期
     */
    private volatile boolean needIncreaseTermImmediately = false;

    /**
     * 最小投票间隔
     */
    private int minVoteIntervalMs = 300;

    /**
     * 最大投票间隔
     */
    private int maxVoteIntervalMs = 1000;

    /**
     * 节点角色状态变更监听器 【无】
     */
    private final List<RoleChangeHandler> roleChangeHandlers = new ArrayList<>();

    /**
     * 上一次投票结果
     */
    private VoteResponse.ParseResult lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;

    /**
     * 上一次投票耗时
     */
    private long lastVoteCost = 0L;

    /**
     * 状态维护后台服务线程
     */
    private final StateMaintainer stateMaintainer = new StateMaintainer("StateMaintainer", logger);

    /**
     * 正在迁移的leader变更任务 【新leader】
     */
    private final TakeLeadershipTask takeLeadershipTask = new TakeLeadershipTask();

    public DLedgerLeaderElector(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerRpcService = dLedgerRpcService;
        refreshIntervals(dLedgerConfig);
    }

    public void startup() {
        stateMaintainer.start();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.startup();
        }
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.shutdown();
        }
    }

    /**
     * 刷新各间隔时间配置
     */
    private void refreshIntervals(DLedgerConfig dLedgerConfig) {
        this.heartBeatTimeIntervalMs = dLedgerConfig.getHeartBeatTimeIntervalMs();
        this.maxHeartBeatLeak = dLedgerConfig.getMaxHeartBeatLeak();
        this.minVoteIntervalMs = dLedgerConfig.getMinVoteIntervalMs();
        this.maxVoteIntervalMs = dLedgerConfig.getMaxVoteIntervalMs();
    }

    //region 切换节点状态到指定角色

    /**
     * 切换角色到leader
     */
    public void changeRoleToLeader(long term) {
        synchronized (memberState) {
            if (memberState.currTerm() == term) {
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1;
                handleRoleChange(term, MemberState.Role.LEADER);
                logger.info("[{}] [ChangeRoleToLeader] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.warn("[{}] skip to be the leader in term: {}, but currTerm is: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    /**
     * 切换角色到candidate
     */
    public void changeRoleToCandidate(long term/* 最大任期 */) {
        synchronized (memberState) {
            if (term >= memberState.currTerm()) {
                memberState.changeToCandidate(term);
                handleRoleChange(term, MemberState.Role.CANDIDATE);
                logger.info("[{}] [ChangeRoleToCandidate] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.info("[{}] skip to be candidate in term: {}, but currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    /**
     * 切换角色到follower
     */
    public void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currTerm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
        handleRoleChange(term, MemberState.Role.FOLLOWER);
    }
    //endregion

    /**
     * 处理leader心跳 【维护心跳时间、防止follower变更为candidate】
     */
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {

        //region 非法的leaderId 【不是当前集群节点ID、leaderId与当前节点相同】
        if (!memberState.isPeerMember(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] remoteId={} is an unknown member", request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNKNOWN_MEMBER.getCode()));
        }

        if (memberState.getSelfId().equals(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode()));
        }
        //endregion

        //region 任期过期 【网络分区、旧leader向新任期中节点发送心跳】
        if (request.getTerm() < memberState.currTerm()) {
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        }
        //endregion

        //region 心跳成功 【任期与leaderId均与本节点相同、更新上次心跳时间后正常返回】
        else if (request.getTerm() == memberState.currTerm()) {
            if (request.getLeaderId().equals(memberState.getLeaderId())) {
                lastLeaderHeartBeatTime = System.currentTimeMillis();
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }
        //endregion

        //region 其他异常情况处理
        //abnormal case
        //hold the lock to get the latest term and leaderId
        synchronized (memberState) {
            //region 任期过期 【网络分区、旧leader向新任期中节点发送心跳】
            if (request.getTerm() < memberState.currTerm()) {
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
            }
            //endregion

            //region 同一任期
            else if (request.getTerm() == memberState.currTerm()) {
                //本节点还没有leader、变更为follower并更新为leader任期 【与leader同期竞选的candidate任期与小于leader或相等】
                if (memberState.getLeaderId() == null) {
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                }
                //任期和leaderId均相同、心跳成功
                else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    //任期相同但leader与心跳leader不相同 【应不存在此状况】
                    //脑裂时 2-1-2 某节点先给一边投票、崩溃恢复重启后再给另一边投票，两边朝代相同leader不同
                    //之后集群网络恢复、A区leader向B区follower发送心跳
                    //this should not happen, but if happened
                    logger.error("[{}][BUG] currTerm {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.INCONSISTENT_LEADER.getCode()));
                }
            }
            //endregion

            //region leader任期过大 【使用leaderId设置切换candidate的集群中最大任期、candidate维护角色时会更新为该任期】
            else {
                //To make it simple, for larger term, do not change to follower immediately
                //first change to candidate, and notify the state-maintainer thread
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //TOOD notify
                return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.TERM_NOT_READY.getCode()));
            }
            //endregion
        }
        //endregion
    }

    /**
     * 处理candidate投票
     */
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean self) {
        //hold the lock to get the latest term, leaderId, ledgerEndIndex
        synchronized (memberState) {
            //region 非法的leaderId 【REJECT_UNKNOWN_LEADER、该节点配置文件中未配置该请求投票的leader节点的ID】
            if (!memberState.isPeerMember(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] remoteId={} is an unknown member", request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNKNOWN_LEADER));
            }
            //endregion

            //region 非法的leaderId【REJECT_UNEXPECTED_LEADER、不是投票给本节点但投票请求中leaderId为本节点ID】
            if (!self && memberState.getSelfId().equals(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNEXPECTED_LEADER));
            }
            //endregion

            //region 竞选节点最后日志的任期落后【REJECT_EXPIRED_LEDGER_TERM、网络分区恢复后落后region发出的投票请求】
            if (request.getLedgerEndTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_LEDGER_TERM));
            }
            //endregion

            //region 竞选节点最后日志的编号落后【REJECT_SMALL_LEDGER_END_INDEX、网络分区恢复后落后region发出的投票请求】
            else if (request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && request.getLedgerEndIndex() < memberState.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_SMALL_LEDGER_END_INDEX));
            }
            //endregion

            //region 竞选节点任期小于当前节点任期 【REJECT_EXPIRED_VOTE_TERM】
            if (request.getTerm() < memberState.currTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            }
            //endregion

            //region 竞选节点任期等于当前节点任期
            else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.currVoteFor() == null) {
                    //该节点未投票过
                    //let it go
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) {
                    //该节点已经接受过该竞选者的投票请求
                    //repeat just let it go
                } else {
                    if (memberState.getLeaderId() != null) {
                        //该任期该节点已经有认定leader
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_HAS_LEADER));
                    } else {
                        //该节点已经接受过别的点的投票请求
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            }
            //endregion

            //region 竞选节点任期大于当前节点任期 【REJECT_TERM_NOT_READY、变更本节点到candidate递增任期】
            else {
                //stepped down by larger term
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }
            //endregion

            //region 竞选节点任期小于本节点最后日志任期 【REJECT_TERM_SMALL_THAN_LEDGER】
            if (request.getTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getLedgerEndTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_SMALL_THAN_LEDGER));
            }
            //endregion

            //region 当前节点为leader转移受让节点、拒绝投票请求
            if (!self && isTakingLeadership() && request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && memberState.getLedgerEndIndex() >= request.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TAKING_LEADERSHIP));
            }
            //endregion

            //region 接受该投票请求 【设置当前节点投票者并返回ACCEPT状态】
            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.ACCEPT));
            //endregion
        }
    }

    /**
     * 是否在切换leader中
     */
    private boolean isTakingLeadership() {
        //currTerm在首次投票中已经递增、故等于TermToTakeLeadership
        if (dLedgerConfig.getPreferredLeaderIds() != null && memberState.getTermToTakeLeadership() == memberState.currTerm()) {
            List<String> preferredLeaderIds = Arrays.asList(dLedgerConfig.getPreferredLeaderIds().split(";"));
            return preferredLeaderIds.contains(memberState.getSelfId());
        }
        return false;
    }

    /**
     * 计算下轮投票时间 【当前时间 + 随机间隔时间(最小投票间隔300ms与最大投票间隔1000ms间)】
     */
    private long getNextTimeToRequestVote() {
        //region 切换leader的follower变更为candidate时需要更快发送投票请求
        if (isTakingLeadership()) {
            return System.currentTimeMillis() + dLedgerConfig.getMinTakeLeadershipVoteIntervalMs() +
                    random.nextInt(dLedgerConfig.getMaxTakeLeadershipVoteIntervalMs() /* 100 */ - dLedgerConfig.getMinTakeLeadershipVoteIntervalMs()/* 30 */);
        }
        //endregion
        return System.currentTimeMillis() + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs/* 1000 */ - minVoteIntervalMs/* 300 */);
    }

    //region 按节点角色维护选举状态

    /**
     * 按节点角色维护选举状态
     * The core method of maintainer. Run the specified logic according to the current role: candidate => propose a
     * vote. leader => send heartbeats to followers, and step down to candidate when quorum followers do not respond.
     * follower => accept heartbeats, and change to candidate when no heartbeat from leader.
     *
     * @throws Exception
     */
    private void maintainState() throws Exception {
        if (memberState.isLeader()) {
            maintainAsLeader();
        } else if (memberState.isFollower()) {
            maintainAsFollower();
        } else {
            maintainAsCandidate();
        }
    }

    /**
     * follower状态维护 【检查是否长时间未收到leader心跳、是则变更节点角色到candidate】
     */
    private void maintainAsFollower() {
        if (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs/* 2000 */) {
            synchronized (memberState) {
                if (memberState.isFollower() && (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > maxHeartBeatLeak/* 3 */ * heartBeatTimeIntervalMs)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {} lastLeader={}", memberState.getSelfId(), new Timestamp(lastLeaderHeartBeatTime), heartBeatTimeIntervalMs, memberState.getLeaderId());
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }

    /**
     * candidate状态维护 【主要竞选任期递增条件: 集群存在大于竞选任期任期或通信正常但未取得大多数投票时】
     */
    private void maintainAsCandidate() throws Exception {
        //for candidate
        /*
         * 达到投票时间或未达到投票时间立即进行投票
         * needIncreaseTermImmediately = true时立即递增任期但不发起投票
         * 接收到心跳、投票请求时发现节点任期落后、或follower被leader受让需增加任期时 needIncreaseTermImmediately = true
         * 进行candidate角色维护时立即递增任期 +1且等待下轮投票时间
         */
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }
        //region 计算投票请求内容 【竞选任期、最后日志任期、最后日志序号】
        long term;
        long ledgerEndTerm;
        long ledgerEndIndex;
        synchronized (memberState) {
            if (!memberState.isCandidate()) {
                return;
            }
            //上一次投票结果未获得大多数同意节点或任期落后需递增任期 或 needIncreaseTermImmediately = true(其他原因表示任期落后或leader迁移)
            if (lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) {
                long prevTerm = memberState.currTerm();
                term = memberState.nextTerm();
                logger.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);
                lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            } else {
                term = memberState.currTerm();
            }
            ledgerEndIndex = memberState.getLedgerEndIndex();
            ledgerEndTerm = memberState.getLedgerEndTerm();
        }
        //endregion

        //region 因needIncreaseTermImmediately = true递增任期需等待下轮投票时间
        if (needIncreaseTermImmediately) {
            nextTimeToRequestVote = getNextTimeToRequestVote();
            needIncreaseTermImmediately = false;
            return;
        }
        //endregion

        long startVoteTimeMs = System.currentTimeMillis();
        CountDownLatch voteLatch = new CountDownLatch(1);

        //region 向所有投票节点发送投票请求
        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, ledgerEndTerm, ledgerEndIndex);
        //参与投票节点中最大任期号
        final AtomicLong knownMaxTermInGroup = new AtomicLong(term);
        //参与投票节点数
        final AtomicInteger allNum = new AtomicInteger(0);
        //未发生异常的节点数
        final AtomicInteger validNum = new AtomicInteger(0);
        //接受竞选请求节点数
        final AtomicInteger acceptedNum = new AtomicInteger(0);
        //任期落后该节点数
        final AtomicInteger notReadyTermNum = new AtomicInteger(0);
        //任期或提案索引大于该节点节点数
        final AtomicInteger biggerLedgerNum = new AtomicInteger(0);
        //该任期已经存在leader节点数
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false);
        for (CompletableFuture<VoteResponse> future : quorumVoteResponses) {
            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    logger.info("[{}][GetVoteResponse] {}", memberState.getSelfId(), JSON.toJSONString(x));
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        validNum.incrementAndGet();
                    }

                    //region 根据响应状态计算投票结果
                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            case ACCEPT:
                                //请求节点接受该投票请求
                                acceptedNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_VOTED:
                                //请求节点已经给其他节点投过票
                            case REJECT_TAKING_LEADERSHIP:
                                break;
                            case REJECT_ALREADY_HAS_LEADER:
                                //请求节点已有leader
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            case REJECT_TERM_SMALL_THAN_LEDGER:
                            case REJECT_EXPIRED_VOTE_TERM:
                                //本竞选节点任期过旧
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            case REJECT_EXPIRED_LEDGER_TERM:
                            case REJECT_SMALL_LEDGER_END_INDEX:
                                //请求节点任期或日志比当前竞选节点新
                                biggerLedgerNum.incrementAndGet();
                                break;
                            case REJECT_TERM_NOT_READY:
                                //请求节点任期太久、需等待任期更新
                                notReadyTermNum.incrementAndGet();
                                break;
                            default:
                                break;

                        }
                    }
                    //endregion

                    //region 无需继续等待投票响应
                    /*
                     *已经有节点有leader表示有节点在本轮投票中已经成为leader、本节点仅需等待接受该leader心跳即可、
                     *已经有大多数节点接受该投票请求
                     *已经接受的加上任期落后的大于大多数、本轮已无法得到大多数节点接受
                     */
                    if (alreadyHasLeader.get()
                            || memberState.isQuorum(acceptedNum.get())
                            || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
                        voteLatch.countDown();
                    }
                    //endregion

                } catch (Throwable t) {
                    logger.error("vote response failed", t);
                } finally {

                    //region 递增请求投票节点总数
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        voteLatch.countDown();
                    }
                    //endregion
                }
            });
        }
        //endregion

        //region 等待请求完成
        try {
            voteLatch.await(2000 + random.nextInt(maxVoteIntervalMs), TimeUnit.MILLISECONDS);
        } catch (Throwable ignore) {

        }
        //endregion

        lastVoteCost = DLedgerUtils.elapsed(startVoteTimeMs);
        VoteResponse.ParseResult parseResult;
        //region 竞选节点任期小于所有节点中最大任期 【需递增任期后继续竞选】
        if (knownMaxTermInGroup.get() > term) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
            changeRoleToCandidate(knownMaxTermInGroup.get());
        }
        //endregion

        //region 该任期已经有leader存在 【仅需等待成为该leader的follower、下次投票时间应加上心跳超时时间】
        else if (alreadyHasLeader.get()) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote() + heartBeatTimeIntervalMs * maxHeartBeatLeak/* 2000*3 */;
        }
        //endregion

        //region 正常通信节点数就小于大多数 【计算下次投票时间后等待重投】
        else if (!memberState.isQuorum(validNum.get())) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }
        //endregion

        //region 任期小于等于竞选任期节点数量小于大多数 【已无法成功、额外等待maxVoteIntervalMs再次重选】
        else if (!memberState.isQuorum(validNum.get() - biggerLedgerNum.get())) {
            //竞选节点任期落后数小于大多数、即任可能竞选成功 【计算下次投票时间后等待再次发起投票】
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote() + maxVoteIntervalMs;
        }
        //endregion

        //region 获得大多数同意、投票成功
        else if (memberState.isQuorum(acceptedNum.get())) {
            parseResult = VoteResponse.ParseResult.PASSED;
        }
        //endregion

        //region 接受数量 + 为准备好数量 > 大多数 【立即重投】
        else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
            //同意和为准备好的节点达到大多数、应立即重新投票
            parseResult = VoteResponse.ParseResult.REVOTE_IMMEDIATELY;
        }
        //endregion

        //region 本轮未得到大多数选票 【需递增竞选任期开始下轮竞选】
        else {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }
        //endregion

        //region 设置上次投票结果
        lastParseResult = parseResult;
        logger.info("[{}] [PARSE_VOTE_RESULT] cost={} term={} memberNum={} allNum={} acceptedNum={} notReadyTermNum={} biggerLedgerNum={} alreadyHasLeader={} maxTerm={} result={}",
                memberState.getSelfId(), lastVoteCost, term, memberState.peerSize(), allNum, acceptedNum, notReadyTermNum, biggerLedgerNum, alreadyHasLeader, knownMaxTermInGroup.get(), parseResult);
        //endregion

        //region 若选举成功切换节点角色为leader
        if (parseResult == VoteResponse.ParseResult.PASSED) {
            logger.info("[{}] [VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            changeRoleToLeader(term);
        }
        //endregion
    }

    /**
     * 向所有节点发送投票请求 【本节点直接调用self投票】
     */
    private List<CompletableFuture<VoteResponse>> voteForQuorumResponses(long term, long ledgerEndTerm, long ledgerEndIndex) throws Exception {
        List<CompletableFuture<VoteResponse>> responses = new ArrayList<>();
        for (String id : memberState.getPeerMap().keySet()) {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setGroup(memberState.getGroup());
            voteRequest.setLedgerEndIndex(ledgerEndIndex);
            voteRequest.setLedgerEndTerm(ledgerEndTerm);
            voteRequest.setLeaderId(memberState.getSelfId());
            voteRequest.setTerm(term);
            voteRequest.setRemoteId(id);
            CompletableFuture<VoteResponse> voteResponse;
            if (memberState.getSelfId().equals(id)) {
                voteResponse = handleVote(voteRequest, true);
            } else {
                //async
                voteResponse = dLedgerRpcService.vote(voteRequest);
            }
            responses.add(voteResponse);

        }
        return responses;
    }

    /**
     * leader状态维护 【判断是否到发送心跳时间、发送心跳】
     * 心跳内容：任期、节点ID(leaderId)、发送心跳时间
     */
    private void maintainAsLeader() throws Exception {
        if (DLedgerUtils.elapsed(lastSendHeartBeatTime) > heartBeatTimeIntervalMs) {
            long term;
            String leaderId;
            synchronized (memberState) {
                if (!memberState.isLeader()) {
                    //stop sending
                    return;
                }
                term = memberState.currTerm();
                leaderId = memberState.getLeaderId();
                lastSendHeartBeatTime = System.currentTimeMillis();
            }
            sendHeartbeats(term, leaderId);
        }
    }

    /**
     * 向所有节点发送心跳 【计算成功数变更节点状态】
     */
    private void sendHeartbeats(long term, String leaderId) throws Exception {
        //发送心跳总数
        final AtomicInteger allNum = new AtomicInteger(1);
        //成功心跳数
        final AtomicInteger succNum = new AtomicInteger(1);
        //朝代落后需同步数据节点数
        final AtomicInteger notReadyNum = new AtomicInteger(0);
        //本leader朝代过期、通信节点中最大的朝代数
        final AtomicLong maxTerm = new AtomicLong(-1);
        //该任期存在其他leader
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false);
        final CountDownLatch beatLatch = new CountDownLatch(1);
        long startHeartbeatTimeMs = System.currentTimeMillis();
        for (String id : memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }
            //region 向所有节点发送心跳
            HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
            heartBeatRequest.setGroup(memberState.getGroup());
            heartBeatRequest.setLocalId(memberState.getSelfId());
            heartBeatRequest.setRemoteId(id);
            heartBeatRequest.setLeaderId(leaderId);
            heartBeatRequest.setTerm(term);
            CompletableFuture<HeartBeatResponse> future = dLedgerRpcService.heartBeat(heartBeatRequest);
            //endregion

            //region 根据响应统计心跳结果
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                        throw ex;
                    }
                    switch (DLedgerResponseCode.valueOf(x.getCode())) {
                        case SUCCESS:
                            //成功心跳数
                            succNum.incrementAndGet();
                            break;
                        case EXPIRED_TERM:
                            //接受心跳节点任期更高
                            maxTerm.set(x.getTerm());
                            break;
                        case INCONSISTENT_LEADER:
                            //接受心跳节点leader不是本leader
                            inconsistLeader.compareAndSet(false, true);
                            break;
                        case TERM_NOT_READY:
                            //接受心跳节点任期落后
                            notReadyNum.incrementAndGet();
                            break;
                        default:
                            break;
                    }

                    //region 更新节点是否活跃状态
                    if (x.getCode() == DLedgerResponseCode.NETWORK_ERROR.getCode()) {
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                    } else {
                        memberState.getPeersLiveTable().put(id, Boolean.TRUE);
                    }
                    //endregion

                    //已经达到了维持leader地位的大多数成功心跳数、无需继续等待其他节点响应
                    if (memberState.isQuorum(succNum.get())
                            || memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                        beatLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("heartbeat response failed", t);
                } finally {
                    //增加发送心跳总数
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        beatLatch.countDown();
                    }
                }
            });
            //endregion
        }
        beatLatch.await(heartBeatTimeIntervalMs, TimeUnit.MILLISECONDS);
        //region 收到了多数节点心跳、保持leader地位、更新最后心跳成功发送时间
        if (memberState.isQuorum(succNum.get())) {
            lastSuccHeartBeatTime = System.currentTimeMillis();
        }
        //endregion

        //region 未收到足够成功心跳
        else {
            logger.info("[{}] Parse heartbeat responses in cost={} term={} allNum={} succNum={} notReadyNum={} inconsistLeader={} maxTerm={} peerSize={} lastSuccHeartBeatTime={}",
                    memberState.getSelfId(), DLedgerUtils.elapsed(startHeartbeatTimeMs), term, allNum.get(), succNum.get(), notReadyNum.get(), inconsistLeader.get(), maxTerm.get(), memberState.peerSize(), new Timestamp(lastSuccHeartBeatTime));
            if (memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                //成功心跳数 + 落后节点数达到大多数、暂时维持leader地位、待下一次再转换到candidate
                lastSendHeartBeatTime = -1;
            } else if (maxTerm.get() > term) {
                //当前leader朝代已落后、节点状态变更为candidate
                changeRoleToCandidate(maxTerm.get());
            } else if (inconsistLeader.get()) {
                //存在其他相同朝代的不同leader、都转变成candidate重新选举
                changeRoleToCandidate(term);
            } else if (DLedgerUtils.elapsed(lastSuccHeartBeatTime) > maxHeartBeatLeak/* 3 */ * heartBeatTimeIntervalMs) {
                //维护心跳长时间未成功、节点状态变更为candidate
                changeRoleToCandidate(term);
            }
        }
        //endregion
    }
    //endregion

    /**
     * 节点角色变更钩子【触发所有角色变更钩子(无)、并检查挂起的leader变更请求(仅受让follower执行)】
     */
    private void handleRoleChange(long term, MemberState.Role role) {
        try {
            takeLeadershipTask.check(term, role);
        } catch (Throwable t) {
            logger.error("takeLeadershipTask.check failed. ter={}, role={}", term, role, t);
        }

        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            try {
                roleChangeHandler.handle(term, role);
            } catch (Throwable t) {
                logger.warn("Handle role change failed term={} role={} handler={}", term, role, roleChangeHandler.getClass(), t);
            }
        }
    }

    /**
     * 增加节点角色变更监听handler 【无】
     */
    public void addRoleChangeHandler(RoleChangeHandler roleChangeHandler) {
        if (!roleChangeHandlers.contains(roleChangeHandler)) {
            roleChangeHandlers.add(roleChangeHandler);
        }
    }

    /**
     * 当前leader处理leader变更 【设置受让节点并转发变更请求给新leader】
     */
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(LeadershipTransferRequest request) throws Exception {
        logger.info("handleLeadershipTransfer: {}", request);
        synchronized (memberState) {
            if (memberState.currTerm() != request.getTerm()) {
                logger.warn("[BUG] [HandleLeaderTransfer] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.INCONSISTENT_TERM.getCode()));
            }

            if (!memberState.isLeader()) {
                logger.warn("[BUG] [HandleLeaderTransfer] selfId={} is not leader", request.getLeaderId());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.NOT_LEADER.getCode()));
            }

            if (memberState.getTransferee() != null) {
                logger.warn("[BUG] [HandleLeaderTransfer] transferee={} is already set", memberState.getTransferee());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.LEADER_TRANSFERRING.getCode()));
            }

            memberState.setTransferee(request.getTransfereeId());
        }
        LeadershipTransferRequest takeLeadershipRequest = new LeadershipTransferRequest();
        takeLeadershipRequest.setGroup(memberState.getGroup());
        takeLeadershipRequest.setLeaderId(memberState.getLeaderId());
        takeLeadershipRequest.setLocalId(memberState.getSelfId());
        takeLeadershipRequest.setRemoteId(request.getTransfereeId());
        takeLeadershipRequest.setTerm(request.getTerm());
        takeLeadershipRequest.setTakeLeadershipLedgerIndex(memberState.getLedgerEndIndex());
        takeLeadershipRequest.setTransferId(memberState.getSelfId());
        takeLeadershipRequest.setTransfereeId(request.getTransfereeId());
        if (memberState.currTerm() != request.getTerm()) {
            logger.warn("[HandleLeaderTransfer] term changed, cur={} , request={}", memberState.currTerm(), request.getTerm());
            return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        }

        return dLedgerRpcService.leadershipTransfer(takeLeadershipRequest).thenApply(response -> {
            synchronized (memberState) {
                if (response.getCode() != DLedgerResponseCode.SUCCESS.getCode() ||
                        (memberState.currTerm() == request.getTerm() && memberState.getTransferee() != null)) {
                    logger.warn("leadershipTransfer failed, set transferee to null");
                    memberState.setTransferee(null);
                }
            }
            return response;
        });
    }

    /**
     * 新leader接收leader变更
     */
    public CompletableFuture<LeadershipTransferResponse> handleTakeLeadership(LeadershipTransferRequest request) throws Exception {
        logger.debug("handleTakeLeadership.request={}", request);
        synchronized (memberState) {
            if (memberState.currTerm() != request.getTerm()) {
                logger.warn("[BUG] [handleTakeLeadership] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.INCONSISTENT_TERM.getCode()));
            }

            long targetTerm = request.getTerm() + 1;
            memberState.setTermToTakeLeadership(targetTerm);
            //挂起变更请求、等待新选举完成
            CompletableFuture<LeadershipTransferResponse> response = new CompletableFuture<>();
            takeLeadershipTask.update(request, response);
            //切换到candidate、新增任期、发起新的选举
            changeRoleToCandidate(targetTerm);
            needIncreaseTermImmediately = true;
            return response;
        }
    }

    /**
     * 受让follower挂起的leader变更请求
     */
    private class TakeLeadershipTask {
        private LeadershipTransferRequest request;
        private CompletableFuture<LeadershipTransferResponse> responseFuture;

        public synchronized void update(LeadershipTransferRequest request, CompletableFuture<LeadershipTransferResponse> responseFuture) {
            this.request = request;
            this.responseFuture = responseFuture;
        }

        //受让follower检查是否已经竞选成leader
        public synchronized void check(long term, MemberState.Role role) {
            logger.trace("TakeLeadershipTask called, term={}, role={}", term, role);
            if (memberState.getTermToTakeLeadership() == -1 || responseFuture == null) {
                return;
            }
            LeadershipTransferResponse response = null;
            if (term > memberState.getTermToTakeLeadership()) {
                response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.EXPIRED_TERM.getCode());
            } else if (term == memberState.getTermToTakeLeadership()) {
                switch (role) {
                    //follower成功成为新leader、响应旧leader切换请求
                    case LEADER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.SUCCESS.getCode());
                        break;
                    //未成为leader、切换失败
                    case FOLLOWER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.TAKE_LEADERSHIP_FAILED.getCode());
                        break;
                    //新选举还未完成
                    default:
                        return;
                }
            } else {
                switch (role) {
                    /**
                     * 刚变为candidate、还未增加目标任期就正好收到心跳再次成为follower、变更leader失败
                     * The node may receive heartbeat before term increase as a candidate,
                     * then it will be follower and term < TermToTakeLeadership
                     */
                    case FOLLOWER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.TAKE_LEADERSHIP_FAILED.getCode());
                        break;
                    default:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.INTERNAL_ERROR.getCode());
                }
            }

            responseFuture.complete(response);
            logger.info("TakeLeadershipTask finished. request={}, response={}, term={}, role={}", request, response, term, role);
            memberState.setTermToTakeLeadership(-1);
            responseFuture = null;
            request = null;
        }
    }

    /**
     * 状态维护后台服务线程 【间隔10ms调用状态维护方法、按节点角色维护选举状态】
     */
    public class StateMaintainer extends ShutdownAbleThread {

        public StateMaintainer(String name, Logger logger) {
            super(name, logger);
        }

        @Override
        public void doWork() {
            try {
                if (DLedgerLeaderElector.this.dLedgerConfig.isEnableLeaderElector()) {
                    DLedgerLeaderElector.this.refreshIntervals(dLedgerConfig);
                    DLedgerLeaderElector.this.maintainState();
                }
                sleep(10);
            } catch (Throwable t) {
                DLedgerLeaderElector.logger.error("Error in heartbeat", t);
            }
        }

    }

    /**
     * 节点角色变更监听处理器
     */
    public interface RoleChangeHandler {
        void handle(long term, MemberState.Role role);

        void startup();

        void shutdown();
    }

    //just for test
    public void testRevote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

}
