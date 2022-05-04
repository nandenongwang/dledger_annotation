package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.protocol.DLedgerProtocol;
import io.openmessaging.storage.dledger.protocol.DLedgerProtocolHandler;

public abstract class DLedgerRpcService implements DLedgerProtocol, DLedgerProtocolHandler {

    public abstract void startup();

    public abstract void shutdown();

}
