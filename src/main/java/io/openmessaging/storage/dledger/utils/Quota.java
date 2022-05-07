package io.openmessaging.storage.dledger.utils;

/**
 * 统计工具
 */
public class Quota {

    /**
     * 采样指标最大值
     */
    private final int max;

    /**
     * 采样值列表
     */
    private final int[] samples;

    /**
     * 采样值列表
     */
    private final long[] timeVec;

    /**
     * 保留多少个采样值
     */
    private final int window;

    public Quota(int max) {
        this(5, max);
    }

    public Quota(int window, int max) {
        if (window < 5) {
            window = 5;
        }
        this.max = max;
        this.window = window;
        this.samples = new int[window];
        this.timeVec = new long[window];
    }

    /**
     * 取模计算采样值在哪格中
     */
    private int index(long currTimeMs) {
        return (int) (second(currTimeMs) % window);
    }

    /**
     * 毫秒转换成秒
     */
    private long second(long currTimeMs) {
        return currTimeMs / 1000;
    }

    /**
     * 采样 【设置所在格值和时间、相同时间累加采样值】
     */
    public void sample(int value) {
        long timeMs = System.currentTimeMillis();
        int index = index(timeMs);
        long second = second(timeMs);
        if (timeVec[index] != second) {
            timeVec[index] = second;
            samples[index] = value;
        } else {
            samples[index] += value;
        }

    }

    /**
     * 本秒内采样值是否超过最大值
     */
    public boolean validateNow() {
        long timeMs = System.currentTimeMillis();
        int index = index(timeMs);
        long second = second(timeMs);
        if (timeVec[index] == second) {
            return samples[index] >= max;
        }
        return false;
    }

    /**
     * 离下一秒还剩多少毫秒
     */
    public int leftNow() {
        long timeMs = System.currentTimeMillis();
        return (int) ((second(timeMs) + 1) * 1000 - timeMs);
    }
}
