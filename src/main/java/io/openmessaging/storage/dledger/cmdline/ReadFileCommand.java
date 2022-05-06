package io.openmessaging.storage.dledger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.entry.DLedgerEntryCoder;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFile;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
import io.openmessaging.storage.dledger.store.file.SelectMmapBufferResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 读取提案命令 【直接读取mappedFile文件】
 * 1、指定index、索引目录读取索引文件
 * 2、指定开始读取pos、读取size、数据目录读取数据文件
 */
public class ReadFileCommand extends BaseCommand {

    private static final Logger logger = LoggerFactory.getLogger(ReadFileCommand.class);

    /**
     * 索引文件或数据文件所在目录
     */
    @Parameter(names = {"--dir", "-d"}, description = "the data dir")
    private String dataDir = null;

    /**
     * 起始查询位置、索引查询时默认从index计算得、数据查询时需指定
     */
    @Parameter(names = {"--pos", "-p"}, description = "the start pos")
    private long pos = 0;

    /**
     * 文件大小、默认数据文件1G、索引文件32M
     */
    @Parameter(names = {"--size", "-s"}, description = "the file size")
    private int size = -1;

    /**
     * 查询索引序号
     */
    @Parameter(names = {"--index", "-i"}, description = "the index")
    private long index = -1L;

    /**
     * 读取数据时是否读取提案内容
     */
    @Parameter(names = {"--body", "-b"}, description = "if read the body")
    private boolean readBody = false;

    @Override
    public void doCommand() {
        if (index != -1) {
            //索引序号 * 索引单元大小 = 总逻辑位置
            pos = index * DLedgerMmapFileStore.INDEX_UNIT_SIZE/* 32 */;
            if (size == -1) {
                size = DLedgerMmapFileStore.INDEX_UNIT_SIZE * 1024 * 1024;
            }
        } else {
            if (size == -1) {
                size = 1024 * 1024 * 1024;
            }
        }
        MmapFileList mmapFileList = new MmapFileList(dataDir, size);
        mmapFileList.load();

        //计算查询位置在哪个mappedFile中
        MmapFile mmapFile = mmapFileList.findMappedFileByOffset(pos);
        if (mmapFile == null) {
            logger.info("Cannot find the file");
            return;
        }
        //pos % size获取在文件中的位置、从该位置读取数据
        SelectMmapBufferResult result = mmapFile.selectMappedBuffer((int) (pos % size));
        ByteBuffer buffer = result.getByteBuffer();
        if (index != -1) {
            logger.info("magic={} pos={} size={} index={} term={}", buffer.getInt(), buffer.getLong(), buffer.getInt(), buffer.getLong(), buffer.getLong());
        } else {
            DLedgerEntry entry = DLedgerEntryCoder.decode(buffer, readBody);
            logger.info(JSON.toJSONString(entry));
        }
    }
}
