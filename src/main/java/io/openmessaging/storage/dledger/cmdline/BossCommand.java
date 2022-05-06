package io.openmessaging.storage.dledger.cmdline;

import com.beust.jcommander.JCommander;
import io.openmessaging.storage.dledger.DLedger;
import io.openmessaging.storage.dledger.DLedgerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * 命令行入口命令
 */
public class BossCommand {

    public static void main(String[] args) {
        Map<String, BaseCommand> commands = new HashMap<>();
        commands.put("append", new AppendCommand());
        commands.put("get", new GetCommand());
        commands.put("readFile", new ReadFileCommand());
        commands.put("leadershipTransfer", new LeadershipTransferCommand());

        JCommander.Builder builder = JCommander.newBuilder();
        builder.addCommand("server", new DLedgerConfig());
        for (String cmd : commands.keySet()) {
            builder.addCommand(cmd, commands.get(cmd));
        }
        JCommander jc = builder.build();
        jc.parse(args);

        //未解析到子命令、打印用法帮助
        if (jc.getParsedCommand() == null) {
            jc.usage();
        }
        //启动server、解析参数启动dledger server
        else if ("server".equals(jc.getParsedCommand())) {
            String[] subArgs = new String[args.length - 1];
            System.arraycopy(args, 1, subArgs, 0, subArgs.length);
            DLedger.main(subArgs);
        }
        //执行指定的子命令
        else {
            BaseCommand command = commands.get(jc.getParsedCommand());
            if (command != null) {
                command.doCommand();
            } else {
                jc.usage();
            }
        }
    }
}
