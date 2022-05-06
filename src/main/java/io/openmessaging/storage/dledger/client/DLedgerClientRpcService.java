package io.openmessaging.storage.dledger.client;

import io.openmessaging.storage.dledger.protocol.DLedgerClientProtocol;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端接口实现
 */
public abstract class DLedgerClientRpcService implements DLedgerClientProtocol {

    /**
     * 通信dledger server节点
     */
    private final Map<String, String> peerMap = new ConcurrentHashMap<>();

    /**
     * 更新dledger server地址
     */
    public void updatePeers(String peers) {
        for (String peerInfo : peers.split(";")) {
            String nodeId = peerInfo.split("-")[0];
            peerMap.put(nodeId, peerInfo.substring(nodeId.length() + 1));
        }
    }

    public void updatePeers(Map<String, String> peers) {
        peerMap.putAll(peers);
    }

    public String getPeerAddr(String id) {
        return peerMap.get(id);
    }

    public abstract void startup();

    public abstract void shutdown();
}
