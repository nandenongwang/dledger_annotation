package io.openmessaging.storage.dledger.utils;

import java.io.File;
import java.text.NumberFormat;
import java.util.Calendar;

public class DLedgerUtils {

    /**
     * 睡眠指定毫秒数
     */
    public static void sleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (Throwable ignored) {

        }
    }

    /**
     * 计算当前时间与指定时间差值
     */
    public static long elapsed(long start) {
        return System.currentTimeMillis() - start;
    }

    /**
     * offset数字格式化成文件名
     */
    public static String offset2FileName(final long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * 计算当前时间与指定时间差值
     */
    public static long computeEclipseTimeMilliseconds(final long beginTime) {
        return System.currentTimeMillis() - beginTime;
    }

    /**
     * 是否到了指定时间 【小时】
     */
    public static boolean isItTimeToDo(final String when) {
        String[] whiles = when.split(";");
        if (whiles.length > 0) {
            Calendar now = Calendar.getInstance();
            for (String w : whiles) {
                int nowHour = Integer.parseInt(w);
                if (nowHour == now.get(Calendar.HOUR_OF_DAY)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 计算指定路径所在盘空间使用率
     */
    public static double getDiskPartitionSpaceUsedPercent(final String path) {
        if (null == path || path.isEmpty()) {
            return -1;
        }

        try {
            File file = new File(path);

            if (!file.exists()) {
                return -1;
            }

            long totalSpace = file.getTotalSpace();

            if (totalSpace > 0) {
                long usedSpace = totalSpace - file.getFreeSpace();
                long usableSpace = file.getUsableSpace();
                long entireSpace = usedSpace + usableSpace;
                long roundNum = 0;
                if (usedSpace * 100 % entireSpace != 0) {
                    roundNum = 1;
                }
                long result = usedSpace * 100 / entireSpace + roundNum;
                return result / 100.0;
            }
        } catch (Exception e) {
            return -1;
        }
        return -1;
    }

    /**
     * 判断文件是否存在
     */
    public static boolean isPathExists(final String path) {
        File file = new File(path);
        return file.exists();
    }
}
