package io.openmessaging.storage.dledger;

import java.util.concurrent.CompletableFuture;

/**
 * 带超时时间得future
 */
public class TimeoutFuture<T> extends CompletableFuture<T> {

    protected long createTimeMs = System.currentTimeMillis();

    protected long timeOutMs = 1000;

    public TimeoutFuture() {

    }

    public TimeoutFuture(long timeOutMs) {
        this.timeOutMs = timeOutMs;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public void setCreateTimeMs(long createTimeMs) {
        this.createTimeMs = createTimeMs;
    }

    public long getTimeOutMs() {
        return timeOutMs;
    }

    public void setTimeOutMs(long timeOutMs) {
        this.timeOutMs = timeOutMs;
    }

    /**
     * future是否超时
     */
    public boolean isTimeOut() {
        return System.currentTimeMillis() - createTimeMs >= timeOutMs;
    }

}
