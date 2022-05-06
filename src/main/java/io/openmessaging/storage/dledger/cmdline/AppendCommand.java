package io.openmessaging.storage.dledger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 增加提案命令
 */
public class AppendCommand extends BaseCommand {

    private static final Logger logger = LoggerFactory.getLogger(AppendCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    /**
     * 提案内容
     */
    @Parameter(names = {"--data", "-d"}, description = "the data to append")
    private String data = "Hello";

    /**
     * 增加次数、默认1
     */
    @Parameter(names = {"--count", "-c"}, description = "append several times")
    private int count = 1;

    @Override
    public void doCommand() {
        DLedgerClient dLedgerClient = new DLedgerClient(group, peers);
        dLedgerClient.startup();
        for (int i = 0; i < count; i++) {
            AppendEntryResponse response = dLedgerClient.append(data.getBytes());
            logger.info("Append Result:{}", JSON.toJSONString(response));
        }
        dLedgerClient.shutdown();
    }
}
