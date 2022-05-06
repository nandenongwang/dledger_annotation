package io.openmessaging.storage.dledger.utils;

import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;

/**
 * 检查表达式、否者格式化异常状态码消息并抛出异常
 */
public class PreConditions {

    public static void check(boolean expression, DLedgerResponseCode code) throws DLedgerException {
        check(expression, code, null);
    }

    public static void check(boolean expression, DLedgerResponseCode code, String message) throws DLedgerException {
        if (!expression) {
            message = message == null ? code.toString() : code.toString() + " " + message;
            throw new DLedgerException(code, message);
        }
    }

    public static void check(boolean expression, DLedgerResponseCode code, String format, Object... args) throws DLedgerException {
        if (!expression) {
            String message = code.toString() + " " + String.format(format, args);
            throw new DLedgerException(code, message);
        }
    }
}
