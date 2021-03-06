package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.openmessaging.storage.dledger.protocol.*;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import org.apache.rocketmq.remoting.netty.*;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A netty implementation of DLedgerRpcService. It should be bi-directional, which means it implements both
 * DLedgerProtocol and DLedgerProtocolHandler.
 */

public class DLedgerRpcNettyService extends DLedgerRpcService {

    private static final Logger logger = LoggerFactory.getLogger(DLedgerRpcNettyService.class);

    /**
     * RPC client
     */
    private final NettyRemotingServer remotingServer;

    /**
     * RPC server
     */
    private final NettyRemotingClient remotingClient;

    /**
     * 节点状态
     */
    private MemberState memberState;

    /**
     * dledger服务
     */
    private DLedgerServer dLedgerServer;

    /**
     * 通用接受请求处理线程池
     */
    private final ExecutorService futureExecutor = Executors.newFixedThreadPool(4, new ThreadFactory() {
        private final AtomicInteger threadIndex = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "FutureExecutor_" + this.threadIndex.incrementAndGet());
        }
    });

    /**
     * 发送投票请求线程池
     */
    private final ExecutorService voteInvokeExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger threadIndex = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "voteInvokeExecutor_" + this.threadIndex.incrementAndGet());
        }
    });

    /**
     * 发送心跳请求线程池
     */
    private final ExecutorService heartBeatInvokeExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger threadIndex = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "heartBeatInvokeExecutor_" + this.threadIndex.incrementAndGet());
        }
    });

    /**
     * 初始化rpc client&server、注册命令处理器
     */
    public DLedgerRpcNettyService(DLedgerServer dLedgerServer) {
        this.dLedgerServer = dLedgerServer;
        this.memberState = dLedgerServer.getMemberState();
        NettyRequestProcessor protocolProcessor = new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                return DLedgerRpcNettyService.this.processRequest(ctx, request);
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        };
        //start the remoting server
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(Integer.parseInt(memberState.getSelfAddr().split(":")[1]));
        this.remotingServer = new NettyRemotingServer(nettyServerConfig, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.METADATA.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.APPEND.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.GET.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.PULL.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.PUSH.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.VOTE.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.HEART_BEAT.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.LEADERSHIP_TRANSFER.getCode(), protocolProcessor, null);

        //start the remoting client
        this.remotingClient = new NettyRemotingClient(new NettyClientConfig(), null);

    }

    /**
     * 根据节点ID获取节点地址
     */
    private String getPeerAddr(RequestOrResponse request) {
        //support different groups in the near future
        return memberState.getPeerAddr(request.getRemoteId());
    }

    /**
     * 向指定节点异步发送心跳请求
     */
    @Override
    public CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) throws Exception {
        CompletableFuture<HeartBeatResponse> future = new CompletableFuture<>();
        heartBeatInvokeExecutor.execute(() -> {
            try {
                RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.HEART_BEAT.getCode(), null);
                wrapperRequest.setBody(JSON.toJSONBytes(request));
                remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                    RemotingCommand responseCommand = responseFuture.getResponseCommand();
                    if (responseCommand != null) {
                        HeartBeatResponse response = JSON.parseObject(responseCommand.getBody(), HeartBeatResponse.class);
                        future.complete(response);
                    } else {
                        logger.error("HeartBeat request time out, {}", request.baseInfo());
                        future.complete(new HeartBeatResponse().code(DLedgerResponseCode.NETWORK_ERROR.getCode()));
                    }
                });
            } catch (Throwable t) {
                logger.error("Send heartBeat request failed, {}", request.baseInfo(), t);
                future.complete(new HeartBeatResponse().code(DLedgerResponseCode.NETWORK_ERROR.getCode()));
            }
        });
        return future;
    }

    /**
     * 向指定节点异步发送投票请求
     */
    @Override
    public CompletableFuture<VoteResponse> vote(VoteRequest request) throws Exception {
        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        voteInvokeExecutor.execute(() -> {
            try {
                RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.VOTE.getCode(), null);
                wrapperRequest.setBody(JSON.toJSONBytes(request));
                remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                    RemotingCommand responseCommand = responseFuture.getResponseCommand();
                    if (responseCommand != null) {
                        VoteResponse response = JSON.parseObject(responseCommand.getBody(), VoteResponse.class);
                        future.complete(response);
                    } else {
                        logger.error("Vote request time out, {}", request.baseInfo());
                        future.complete(new VoteResponse());
                    }
                });
            } catch (Throwable t) {
                logger.error("Send vote request failed, {}", request.baseInfo(), t);
                future.complete(new VoteResponse());
            }
        });
        return future;
    }

    /**
     * 【服务端无】
     */
    @Override
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        GetEntriesResponse entriesResponse = new GetEntriesResponse();
        entriesResponse.setCode(DLedgerResponseCode.UNSUPPORTED.getCode());
        return CompletableFuture.completedFuture(entriesResponse);
    }

    /**
     * 新增提案 【未使用、client使用客户端clientRpcServer新增、服务端只push提案】
     */
    @Override
    public CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception {
        CompletableFuture<AppendEntryResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.APPEND.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();
                if (responseCommand != null) {
                    AppendEntryResponse response = JSON.parseObject(responseCommand.getBody(), AppendEntryResponse.class);
                    future.complete(response);
                } else {
                    AppendEntryResponse response = new AppendEntryResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                    future.complete(response);
                }
            });
        } catch (Throwable t) {
            logger.error("Send append request failed, {}", request.baseInfo(), t);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }
        return future;
    }

    /**
     * 【服务端无】
     */
    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception {
        MetadataResponse metadataResponse = new MetadataResponse();
        metadataResponse.setCode(DLedgerResponseCode.UNSUPPORTED.getCode());
        return CompletableFuture.completedFuture(metadataResponse);
    }

    /**
     * 主动拉取提案 【不支持】
     */
    @Override
    public CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.PULL.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = remotingClient.invokeSync(getPeerAddr(request), wrapperRequest, 3000);
        PullEntriesResponse response = JSON.parseObject(wrapperResponse.getBody(), PullEntriesResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    /**
     * leader向follower推送提案
     */
    @Override
    public CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception {
        CompletableFuture<PushEntryResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.PUSH.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();
                if (responseCommand != null) {
                    PushEntryResponse response = JSON.parseObject(responseCommand.getBody(), PushEntryResponse.class);
                    future.complete(response);
                } else {
                    PushEntryResponse response = new PushEntryResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                    future.complete(response);
                }
            });
        } catch (Throwable t) {
            logger.error("Send push request failed, {}", request.baseInfo(), t);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }

        return future;
    }

    /**
     * 当前leader向被转让leader发送转让请求
     */
    @Override
    public CompletableFuture<LeadershipTransferResponse> leadershipTransfer(LeadershipTransferRequest request) throws Exception {
        CompletableFuture<LeadershipTransferResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.LEADERSHIP_TRANSFER.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();
                if (responseCommand != null) {
                    LeadershipTransferResponse response = JSON.parseObject(responseFuture.getResponseCommand().getBody(), LeadershipTransferResponse.class);
                    future.complete(response);
                } else {
                    LeadershipTransferResponse response = new LeadershipTransferResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                    future.complete(response);
                }
            });
        } catch (Throwable t) {
            logger.error("Send leadershipTransfer request failed, {}", request.baseInfo(), t);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }

        return future;
    }

    /**
     * 写出请求响应
     */
    private void writeResponse(RequestOrResponse storeResp, Throwable t, RemotingCommand request, ChannelHandlerContext ctx) {
        RemotingCommand response = null;
        try {
            if (t != null) {
                //the t should not be null, using error code instead
                throw t;
            } else {
                response = handleResponse(storeResp, request);
                response.markResponseType();
                ctx.writeAndFlush(response);
            }
        } catch (Throwable e) {
            logger.error("Process request over, but fire response failed, request:[{}] response:[{}]", request, response, e);
        }
    }

    /**
     * 分派处理各类rpc请求
     * The core method to handle rpc requests. The advantages of using future instead of callback:
     * <p>
     * 1. separate the caller from actual executor, which make it able to handle the future results by the caller's wish
     * 2. simplify the later execution method
     * <p>
     * CompletableFuture is an excellent choice, whenCompleteAsync will handle the response asynchronously. With an
     * independent thread-pool, it will improve performance and reduce blocking points.
     *
     * @param ctx
     * @param request
     * @return
     * @throws Exception
     */
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        DLedgerRequestCode requestCode = DLedgerRequestCode.valueOf(request.getCode());
        switch (requestCode) {
            case METADATA: {
                MetadataRequest metadataRequest = JSON.parseObject(request.getBody(), MetadataRequest.class);
                CompletableFuture<MetadataResponse> future = handleMetadata(metadataRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case APPEND: {
                AppendEntryRequest appendEntryRequest = JSON.parseObject(request.getBody(), AppendEntryRequest.class);
                CompletableFuture<AppendEntryResponse> future = handleAppend(appendEntryRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case GET: {
                GetEntriesRequest getEntriesRequest = JSON.parseObject(request.getBody(), GetEntriesRequest.class);
                CompletableFuture<GetEntriesResponse> future = handleGet(getEntriesRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case PULL: {
                PullEntriesRequest pullEntriesRequest = JSON.parseObject(request.getBody(), PullEntriesRequest.class);
                CompletableFuture<PullEntriesResponse> future = handlePull(pullEntriesRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case PUSH: {
                PushEntryRequest pushEntryRequest = JSON.parseObject(request.getBody(), PushEntryRequest.class);
                CompletableFuture<PushEntryResponse> future = handlePush(pushEntryRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case VOTE: {
                VoteRequest voteRequest = JSON.parseObject(request.getBody(), VoteRequest.class);
                CompletableFuture<VoteResponse> future = handleVote(voteRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case HEART_BEAT: {
                HeartBeatRequest heartBeatRequest = JSON.parseObject(request.getBody(), HeartBeatRequest.class);
                CompletableFuture<HeartBeatResponse> future = handleHeartBeat(heartBeatRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case LEADERSHIP_TRANSFER: {
                long start = System.currentTimeMillis();
                LeadershipTransferRequest leadershipTransferRequest = JSON.parseObject(request.getBody(), LeadershipTransferRequest.class);
                CompletableFuture<LeadershipTransferResponse> future = handleLeadershipTransfer(leadershipTransferRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                    logger.info("LEADERSHIP_TRANSFER FINISHED. Request={}, response={}, cost={}ms",
                            request, x, DLedgerUtils.elapsed(start));
                }, futureExecutor);
                break;
            }
            default:
                logger.error("Unknown request code {} from {}", request.getCode(), request);
                break;
        }
        return null;
    }

    /**
     * 处理client手动转移leader请求
     */
    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(LeadershipTransferRequest leadershipTransferRequest) throws Exception {
        return dLedgerServer.handleLeadershipTransfer(leadershipTransferRequest);
    }

    /**
     * 处理leader心跳请求
     */
    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        return dLedgerServer.handleHeartBeat(request);
    }

    /**
     * 处理candidate投票氢气
     */
    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        VoteResponse response = dLedgerServer.handleVote(request).get();
        return CompletableFuture.completedFuture(response);
    }

    /**
     * 处理client新增提案请求
     */
    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception {
        return dLedgerServer.handleAppend(request);
    }

    /**
     * 处理client查询提案请求
     */
    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        return dLedgerServer.handleGet(request);
    }

    /**
     * 处理client元数据查询请求
     */
    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        return dLedgerServer.handleMetadata(request);
    }

    /**
     * 处理follower拉取提案请求 【不支持】
     */
    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        return dLedgerServer.handlePull(request);
    }

    /**
     * 处理leader推送提案请求
     */
    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return dLedgerServer.handlePush(request);
    }

    /**
     * 封装请求对应响应
     */
    public RemotingCommand handleResponse(RequestOrResponse response, RemotingCommand request) {
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(DLedgerResponseCode.SUCCESS.getCode(), null);
        remotingCommand.setBody(JSON.toJSONBytes(response));
        remotingCommand.setOpaque(request.getOpaque());
        return remotingCommand;
    }

    @Override
    public void startup() {
        this.remotingServer.start();
        this.remotingClient.start();
    }

    @Override
    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingClient.shutdown();
    }

    public MemberState getMemberState() {
        return memberState;
    }

    public void setMemberState(MemberState memberState) {
        this.memberState = memberState;
    }

    public DLedgerServer getdLedgerServer() {
        return dLedgerServer;
    }

    public void setdLedgerServer(DLedgerServer dLedgerServer) {
        this.dLedgerServer = dLedgerServer;
    }
}
