package io.openmessaging.storage.dledger.protocol;

import java.util.HashMap;
import java.util.Map;

public enum DLedgerRequestCode {
    UNKNOWN(-1, ""),
    METADATA(50000, ""),
    APPEND(50001, ""),
    GET(50002, ""),
    VOTE(51001, ""),
    HEART_BEAT(51002, ""),
    PULL(51003, ""),
    PUSH(51004, ""),
    LEADERSHIP_TRANSFER(51005, "");

    private static final Map<Integer, DLedgerRequestCode> codeMap = new HashMap<>();

    static {
        for (DLedgerRequestCode requestCode : DLedgerRequestCode.values()) {
            codeMap.put(requestCode.code, requestCode);
        }
    }

    private final int code;
    private final String desc;

    DLedgerRequestCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static DLedgerRequestCode valueOf(int code) {
        DLedgerRequestCode tmp = codeMap.get(code);
        if (tmp != null) {
            return tmp;
        } else {
            return UNKNOWN;
        }

    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        return String.format("[code=%d,name=%s,desc=%s]", code, name(), desc);
    }

}
