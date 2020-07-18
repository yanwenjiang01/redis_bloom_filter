package com.ict.bloom.filter.utils;

import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.ict.bloom.filter.consts.Consts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author : DevWenjiang
 * Description: Redis布隆过滤器
 * @date : 2020/7/18-17:50
 */
public class RedisBloomFilter {
    //日志
    private Logger logger = LoggerFactory.getLogger(RedisBloomFilter.class);

    private long expectedElementNums;//预期元素数量
    private double fpp;//可容忍的假阳性概率
    private int bitmapLength;//bit数组长度
    private int hashFunctions;//hash函数个数


    //构造方法:【计算出最优bit数组长度及最优hash函数】
    public RedisBloomFilter(long expectedElementNums, double fpp) {
        this.expectedElementNums = expectedElementNums;
        this.fpp = fpp;
        //根据公式计算最优字节长度及最优hash函数个数
        bitmapLength = (int) GuavaUtils.optimalNumOfBits(expectedElementNums, fpp);
        hashFunctions = GuavaUtils.optimalNumOfHashFunctions(expectedElementNums,bitmapLength);
    }

    public int getBitmapLength() {
        return bitmapLength;
    }

    public int getHashFunctions() {
        return hashFunctions;
    }

    /**
     * 计算一个元素值哈希后映射到Bitmap的哪些bit上
     * @param element 元素值
     * @return bit下标的数组
     */
    private long[] getBitIndices(String element) {
        long[] indices = new long[hashFunctions];

        byte[] bytes = Hashing.murmur3_128()
                .hashObject(element, Funnels.stringFunnel(Charset.forName("UTF-8")))
                .asBytes();

        long hash1 = Longs.fromBytes(
                bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]
        );
        long hash2 = Longs.fromBytes(
                bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]
        );

        long combinedHash = hash1;
        for (int i = 0; i < hashFunctions; i++) {
            indices[i] = (combinedHash & Long.MAX_VALUE) % bitmapLength;
            combinedHash += hash2;
        }
        return indices;
    }

    /**
     * 插入元素
     *
     * @param key       原始Redis键，会自动加上'bf:'前缀
     * @param element   元素值，字符串类型
     * @param expireSec 过期时间（秒）
     */
    public void insert(String key, String element, int expireSec,RedisTemplate<String,String> redisTemplate) {
        if (key == null || element == null) {
            throw new RuntimeException("键值均不能为空");
        }
        String actualKey = Consts.REDIS_BLOOM_FILTER_KEY.concat(key);
        List<Object> objects = redisTemplate.executePipelined(new SessionCallback() {
            @Override
            public Object execute(RedisOperations redisOperations) throws DataAccessException {
                if (getBitIndices(element) != null && getBitIndices(element).length != 0) {
                    for (long index : getBitIndices(element)) {
                        redisOperations.opsForValue().setBit(actualKey, index, true);
                        redisOperations.expire(actualKey, expireSec, TimeUnit.SECONDS);
                    }
                }
                return null;
            }
        });
    }

    /**
     * 检查元素在集合中是否（可能）存在
     *
     * @param key     原始Redis键，会自动加上'bf:'前缀
     * @param element 元素值，字符串类型
     */
    public boolean mayExist(String key, String element,RedisTemplate<String,String> redisTemplate) {
        if (key == null || element == null) {
            throw new RuntimeException("键值均不能为空");
        }
        String actualKey = Consts.REDIS_BLOOM_FILTER_KEY.concat(key);
        boolean result = false;

        List<Object> objects = redisTemplate.executePipelined(new SessionCallback() {
            @Override
            public Object execute(RedisOperations redisOperations) throws DataAccessException {
                if (getBitIndices(element) != null && getBitIndices(element).length != 0) {
                    for (long index : getBitIndices(element)) {
                        redisOperations.opsForValue().getBit(actualKey, index);
                    }
                }
                return null;
            }
        });
        for (Object o : objects){
            if (!o.equals( false)){
                result = true;
            }
        }
        return result;
    }



}
