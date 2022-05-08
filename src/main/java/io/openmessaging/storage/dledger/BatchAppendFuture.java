package io.openmessaging.storage.dledger;

public class BatchAppendFuture<T> extends AppendFuture<T> {
    private long[] positions;

    public BatchAppendFuture() {

    }

    public BatchAppendFuture(long timeOutMs) {
        super(timeOutMs);
    }

    public long[] getPositions() {
        return positions;
    }

    public void setPositions(long[] positions) {
        this.positions = positions;
    }

    public static <T> BatchAppendFuture<T> newCompletedFuture(long pos, T value) {
        BatchAppendFuture<T> future = new BatchAppendFuture<T>();
        future.setPos(pos);
        future.complete(value);
        return future;
    }
}