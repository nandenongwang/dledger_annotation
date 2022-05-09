package io.openmessaging.storage.dledger.entry;

import lombok.Data;

@Data
public class DLedgerEntry {

    public final static int POS_OFFSET = 4 + 4 + 8 + 8;
    public final static int HEADER_SIZE = POS_OFFSET + 8 + 4 + 4 + 4;
    public final static int BODY_OFFSET = HEADER_SIZE + 4;

    private int magic;
    private int size;
    private long index;
    private long term;
    private long pos; //used to validate data
    private int channel; //reserved
    private int chainCrc; //like the block chain, this crc indicates any modification before this entry.
    private int bodyCrc; //the crc of the body
    private byte[] body;

    public int computeSizeInBytes() {
        size = HEADER_SIZE + 4 + body.length;
        return size;
    }

}
