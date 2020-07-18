package com.ict.bloom.filter.utils;

/**
 * @author : DevWenjiang
 * Description: Guava中布隆过滤器实现
 * @date : 2020/7/18-18:25
 */
public class GuavaUtils {
    /**
     * 根据预期元素长度及可容忍错误率计算最优字节长度
     * @param expectedElementNums
     * @param fpp
     * @return
     */
    public static long optimalNumOfBits(long expectedElementNums, double fpp) {
        if (fpp == 0.0D) {
            fpp = 4.9E-324D;
        }
        return (long)((double)(-expectedElementNums) * Math.log(fpp) / (Math.log(2.0D) * Math.log(2.0D)));
    }

    /**
     * 根据最优长度及预期元素数量算出最优hash数
     * @param expectedElementNums
     * @param bitmapLength
     * @return
     */
    public static int optimalNumOfHashFunctions(long expectedElementNums, long bitmapLength) {
        return Math.max(1, (int) Math.round((double) bitmapLength /(double) expectedElementNums * Math.log(2.0D)));
    }
}
