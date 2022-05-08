package io.openmessaging.storage.dledger;

import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import lombok.Getter;
import lombok.Setter;

import java.io.File;

public class DLedgerConfig {

    public static final String MEMORY = "MEMORY";
    public static final String FILE = "FILE";
    public static final String MULTI_PATH_SPLITTER = System.getProperty("dLedger.multiPath.Splitter", ",");

    /**
     * 组名 默认default
     */
    @Getter
    @Setter
    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    /**
     * 本机ID
     */
    @Getter
    @Setter
    @Parameter(names = {"--id", "-i"}, description = "Self id of this server")
    private String selfId = "n0";

    /**
     * 所有节点 id1-addr1;id2-addr2:id3-addr3
     */
    @Getter
    @Setter
    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--store-base-dir", "-s"}, description = "The base store dir of this server")
    @Getter
    @Setter
    private String storeBaseDir = File.separator + "tmp" + File.separator + "dledgerstore";
    @Getter
    @Setter
    @Parameter(names = {"--read-only-data-store-dirs"}, description = "The dirs of this server to be read only")
    private String readOnlyDataStoreDirs = null;

    @Parameter(names = {"--peer-push-throttle-point"}, description = "When the follower is behind the leader more than this value, it will trigger the throttle")
    @Getter
    @Setter
    private int peerPushThrottlePoint = 300 * 1024 * 1024;
    @Getter
    @Setter
    @Parameter(names = {"--peer-push-quotas"}, description = "The quotas of the pusher")
    private int peerPushQuota = 20 * 1024 * 1024;

    /**
     * 默认存储类型、文件或内存
     */
    @Getter
    @Setter
    private String storeType = FILE; //FILE, MEMORY
    private String dataStorePath;
    @Getter
    @Setter
    private int maxPendingRequestsNum = 10000;
    @Getter
    @Setter
    private int maxWaitAckTimeMs = 2500;
    @Getter
    @Setter
    private int maxPushTimeOutMs = 1000;
    @Getter
    @Setter
    private boolean enableLeaderElector = true;
    @Getter
    @Setter
    private int heartBeatTimeIntervalMs = 2000;
    @Getter
    @Setter
    private int maxHeartBeatLeak = 3;
    @Getter
    @Setter
    private int minVoteIntervalMs = 300;
    @Getter
    @Setter
    private int maxVoteIntervalMs = 1000;
    @Getter
    @Setter
    private int fileReservedHours = 72;
    @Getter
    @Setter
    private String deleteWhen = "04";
    @Getter
    @Setter
    private float diskSpaceRatioToCheckExpired = Float.parseFloat(System.getProperty("dledger.disk.ratio.check", "0.70"));
    private float diskSpaceRatioToForceClean = Float.parseFloat(System.getProperty("dledger.disk.ratio.clean", "0.85"));
    @Getter
    @Setter
    private boolean enableDiskForceClean = true;
    @Getter
    @Setter
    private long flushFileInterval = 10;
    @Getter
    @Setter
    private long checkPointInterval = 3000;
    @Getter
    @Setter
    private int mappedFileSizeForEntryData = 1024 * 1024 * 1024;
    @Getter
    @Setter
    private int mappedFileSizeForEntryIndex = DLedgerMmapFileStore.INDEX_UNIT_SIZE * 5 * 1024 * 1024;
    @Getter
    @Setter
    private boolean enablePushToFollower = true;

    /**
     * 优先leader节点
     */
    @Parameter(names = {"--preferred-leader-id"}, description = "Preferred LeaderId")
    private String preferredLeaderIds;

    /**
     * leader转移受让节点最多落后进度
     */
    @Getter
    @Setter
    private long maxLeadershipTransferWaitIndex = 1000;
    @Getter
    @Setter
    private int minTakeLeadershipVoteIntervalMs = 30;
    @Getter
    @Setter
    private int maxTakeLeadershipVoteIntervalMs = 100;
    @Getter
    @Setter
    private boolean isEnableBatchPush = false;
    @Getter
    @Setter
    private int maxBatchPushSize = 4 * 1024;

    /**
     * leader转移等待新leader进度追上旧leader超时
     */
    @Getter
    @Setter
    private long leadershipTransferWaitTimeout = 1000;

    public String getDefaultPath() {
        return storeBaseDir + File.separator + "dledger-" + selfId;
    }

    public String getDataStorePath() {
        if (dataStorePath == null) {
            return getDefaultPath() + File.separator + "data";
        }
        return dataStorePath;
    }

    public void setDataStorePath(String dataStorePath) {
        this.dataStorePath = dataStorePath;
    }

    public String getIndexStorePath() {
        return getDefaultPath() + File.separator + "index";
    }

    //for builder semantic
    public DLedgerConfig group(String group) {
        this.group = group;
        return this;
    }

    public DLedgerConfig selfId(String selfId) {
        this.selfId = selfId;
        return this;
    }

    public DLedgerConfig peers(String peers) {
        this.peers = peers;
        return this;
    }

    public DLedgerConfig storeBaseDir(String dir) {
        this.storeBaseDir = dir;
        return this;
    }

    public float getDiskSpaceRatioToForceClean() {
        if (diskSpaceRatioToForceClean < 0.50f) {
            return 0.50f;
        } else {
            return diskSpaceRatioToForceClean;
        }
    }

    public void setDiskSpaceRatioToForceClean(float diskSpaceRatioToForceClean) {
        this.diskSpaceRatioToForceClean = diskSpaceRatioToForceClean;
    }

    public float getDiskFullRatio() {
        float ratio = diskSpaceRatioToForceClean + 0.05f;
        if (ratio > 0.95f) {
            return 0.95f;
        }
        return ratio;
    }

    @Deprecated
    public String getPreferredLeaderId() {
        return preferredLeaderIds;
    }

    @Deprecated
    public void setPreferredLeaderId(String preferredLeaderId) {
        this.preferredLeaderIds = preferredLeaderId;
    }

    public String getPreferredLeaderIds() {
        return preferredLeaderIds;
    }

    public void setPreferredLeaderIds(String preferredLeaderIds) {
        this.preferredLeaderIds = preferredLeaderIds;
    }
}
