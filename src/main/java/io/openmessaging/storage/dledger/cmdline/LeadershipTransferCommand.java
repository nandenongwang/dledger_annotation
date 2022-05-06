package io.openmessaging.storage.dledger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * leader手动变更命令
 */
public class LeadershipTransferCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(LeadershipTransferCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    /**
     * 当前leader节点ID
     */
    @Parameter(names = {"--leader", "-l"}, description = "set the current leader manually")
    private String leaderId;

    /**
     * 新leader节点ID
     */
    @Parameter(names = {"--transfereeId", "-t"}, description = "Node try to be the new leader")
    private String transfereeId = "n0";

    /**
     * 当前leader任期
     */
    @Parameter(names = {"--term"}, description = "current term")
    private long term;

    @Override
    public void doCommand() {
        DLedgerClient dLedgerClient = new DLedgerClient(group, peers);
        dLedgerClient.startup();
        LeadershipTransferResponse response = dLedgerClient.leadershipTransfer(leaderId, transfereeId, term);
        logger.info("LeadershipTransfer code={}, Result:{}", DLedgerResponseCode.valueOf(response.getCode()), JSON.toJSONString(response));
        dLedgerClient.shutdown();
    }
}
