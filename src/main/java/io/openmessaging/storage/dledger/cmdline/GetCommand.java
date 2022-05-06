package io.openmessaging.storage.dledger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 查询提案命令
 */
public class GetCommand extends BaseCommand {

    private static final Logger logger = LoggerFactory.getLogger(GetCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    /**
     * 提案序号
     */
    @Parameter(names = {"--index", "-i"}, description = "get entry from index")
    private long index = 0;

    @Override
    public void doCommand() {
        DLedgerClient dLedgerClient = new DLedgerClient(group, peers);
        dLedgerClient.startup();
        GetEntriesResponse response = dLedgerClient.get(index);
        logger.info("Get Result:{}", JSON.toJSONString(response));
        if (response.getEntries() != null && response.getEntries().size() > 0) {
            for (DLedgerEntry entry : response.getEntries()) {
                logger.info("Get Result index:{} {}", entry.getIndex(), new String(entry.getBody()));
            }
        }
        dLedgerClient.shutdown();
    }
}
