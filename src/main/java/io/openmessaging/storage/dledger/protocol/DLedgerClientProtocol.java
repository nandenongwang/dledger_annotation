package io.openmessaging.storage.dledger.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * 客户端接口
 * Both the RaftLogServer(inbound) and RaftRpcService (outbound) should implement this protocol
 */
public interface DLedgerClientProtocol {

    /**
     * 查询提案
     */
    CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception;

    /**
     * 增加提案
     */
    CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception;

    /**
     * 元数据查询
     */
    CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception;

    /**
     * 手动转移leader
     */
    CompletableFuture<LeadershipTransferResponse> leadershipTransfer(LeadershipTransferRequest request) throws Exception;
}
