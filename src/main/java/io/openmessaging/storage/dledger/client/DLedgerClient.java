package io.openmessaging.storage.dledger.client;

import io.openmessaging.storage.dledger.ShutdownAbleThread;
import io.openmessaging.storage.dledger.protocol.*;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * dledger客户端
 */
public class DLedgerClient {

    private static final Logger logger = LoggerFactory.getLogger(DLedgerClient.class);

    /**
     * 客户端配置中的dledger server节点
     */
    private final Map<String/* id */, String/* addr */> peerMap = new ConcurrentHashMap<>();

    /**
     * group
     */
    private final String group;

    /**
     * leader节点ID
     */
    private String leaderId;

    /**
     * 客户端功能接口实现
     */
    private final DLedgerClientRpcService dLedgerClientRpcService;

    /**
     * 元数据更新后台线程
     */
    private final MetadataUpdater metadataUpdater = new MetadataUpdater("MetadataUpdater", logger);

    public DLedgerClient(String group, String peers) {
        this.group = group;
        updatePeers(peers);
        dLedgerClientRpcService = new DLedgerClientRpcNettyService();
        dLedgerClientRpcService.updatePeers(peers);
        leaderId = peerMap.keySet().iterator().next();
    }

    /**
     * 增加提案
     */
    public AppendEntryResponse append(byte[] body) {
        try {
            //region 更新server地址
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return appendEntryResponse;
            }
            //endregion

            //region 向leader发送增加提案请求
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(leaderId);
            appendEntryRequest.setBody(body);
            AppendEntryResponse response = dLedgerClientRpcService.append(appendEntryRequest).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    appendEntryRequest.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.append(appendEntryRequest).get();
                }
            }
            //endregion
            return response;
        } catch (Exception e) {
            needFreshMetadata();
            logger.error("{}", e);
            AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
            appendEntryResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return appendEntryResponse;
        }
    }

    /**
     * 查询提案
     */
    public GetEntriesResponse get(long index) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                GetEntriesResponse response = new GetEntriesResponse();
                response.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return response;
            }

            GetEntriesRequest request = new GetEntriesRequest();
            request.setGroup(group);
            request.setRemoteId(leaderId);
            request.setBeginIndex(index);
            GetEntriesResponse response = dLedgerClientRpcService.get(request).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    request.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.get(request).get();
                }
            }
            return response;
        } catch (Exception t) {
            needFreshMetadata();
            logger.error("", t);
            GetEntriesResponse getEntriesResponse = new GetEntriesResponse();
            getEntriesResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return getEntriesResponse;
        }
    }

    /**
     * leader转移
     */
    public LeadershipTransferResponse leadershipTransfer(String curLeaderId, String transfereeId, long term) {

        try {
            LeadershipTransferRequest request = new LeadershipTransferRequest();
            request.setGroup(group);
            request.setRemoteId(curLeaderId);
            request.setTransferId(curLeaderId);
            request.setTransfereeId(transfereeId);
            request.setTerm(term);
            return dLedgerClientRpcService.leadershipTransfer(request).get();
        } catch (Exception t) {
            needFreshMetadata();
            logger.error("leadershipTransfer to {} error", transfereeId, t);
            return new LeadershipTransferResponse().code(DLedgerResponseCode.INTERNAL_ERROR.getCode());
        }
    }

    /**
     * 启动客户端 【api server & 元数据更新线程】
     */
    public void startup() {
        this.dLedgerClientRpcService.startup();
        this.metadataUpdater.start();
    }

    /**
     * 关闭客户端 【api server & 元数据更新线程】
     */
    public void shutdown() {
        this.dLedgerClientRpcService.shutdown();
        this.metadataUpdater.shutdown();
    }

    /**
     * 设置client配置的dledger server地址
     */
    private void updatePeers(String peers) {
        for (String peerInfo : peers.split(";")) {
            String nodeId = peerInfo.split("-")[0];
            peerMap.put(nodeId, peerInfo.substring(nodeId.length() + 1));
        }
    }

    /**
     * 唤醒元数据更新线程拉取dledger server节点
     */
    private synchronized void needFreshMetadata() {
        leaderId = null;
        metadataUpdater.wakeup();
    }

    /**
     * 延时更新元数据
     */
    private synchronized void waitOnUpdatingMetadata(long maxWaitMs, boolean needFresh) {
        if (needFresh) {
            leaderId = null;
        } else if (leaderId != null) {
            return;
        }
        long start = System.currentTimeMillis();
        while (DLedgerUtils.elapsed(start) < maxWaitMs && leaderId == null) {
            metadataUpdater.wakeup();
            try {
                wait(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    /**
     * 元数据更新后台线程 【定时拉取、从配置server地址拉取所有dledger server地址和leaderId】
     */
    private class MetadataUpdater extends ShutdownAbleThread {

        public MetadataUpdater(String name, Logger logger) {
            super(name, logger);
        }

        private void getMetadata(String peerId, boolean isLeader) {
            try {
                MetadataRequest request = new MetadataRequest();
                request.setGroup(group);
                request.setRemoteId(peerId);
                CompletableFuture<MetadataResponse> future = dLedgerClientRpcService.metadata(request);
                MetadataResponse response = future.get(1500, TimeUnit.MILLISECONDS);
                if (response.getLeaderId() != null) {
                    leaderId = response.getLeaderId();
                    if (response.getPeers() != null) {
                        peerMap.putAll(response.getPeers());
                        dLedgerClientRpcService.updatePeers(response.getPeers());
                    }
                }
            } catch (Throwable t) {
                if (isLeader) {
                    needFreshMetadata();
                }
                logger.warn("Get metadata failed from {}", peerId, t);
            }

        }

        @Override
        public void doWork() {
            try {
                if (leaderId == null) {
                    for (String peer : peerMap.keySet()) {
                        getMetadata(peer, false);
                        if (leaderId != null) {
                            synchronized (DLedgerClient.this) {
                                DLedgerClient.this.notifyAll();
                            }
                            DLedgerUtils.sleep(1000);
                            break;
                        }
                    }
                } else {
                    getMetadata(leaderId, true);
                }
                waitForRunning(3000);
            } catch (Throwable t) {
                logger.error("Error", t);
                DLedgerUtils.sleep(1000);
            }
        }
    }

}
