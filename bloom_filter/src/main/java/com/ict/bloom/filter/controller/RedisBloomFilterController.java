package com.ict.bloom.filter.controller;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Hashing;
import com.ict.bloom.filter.utils.RedisBloomFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @author : DevWenjiang
 * Description:
 * @date : 2020/7/18-19:04
 */
@Controller
public class RedisBloomFilterController {
    @Autowired
    private RedisTemplate<String,String> redisTemplate;

    @RequestMapping("/test")
    public void test(){
        final int NUM_APPROX_ELEMENTS = 3000;
        final double FPP = 0.03;
        final int DAY_SEC = 60 * 60 * 24;
        RedisBloomFilter redisBloomFilter;

        redisBloomFilter = new RedisBloomFilter(NUM_APPROX_ELEMENTS, FPP);
        System.out.println("numHashFunctions: " + redisBloomFilter.getHashFunctions());
        System.out.println("bitmapLength: " + redisBloomFilter.getBitmapLength());


        redisBloomFilter.insert("topic_read:8839540:20190609", "76930242", DAY_SEC,redisTemplate);
        redisBloomFilter.insert("topic_read:8839540:20190609", "76930243", DAY_SEC,redisTemplate);
        redisBloomFilter.insert("topic_read:8839540:20190609", "76930244", DAY_SEC,redisTemplate);
        redisBloomFilter.insert("topic_read:8839540:20190609", "76930245", DAY_SEC,redisTemplate);
        redisBloomFilter.insert("topic_read:8839540:20190609", "76930246", DAY_SEC,redisTemplate);
        System.out.println(redisBloomFilter.mayExist("topic_read:8839540:20190609", "76930242",redisTemplate));
        System.out.println(redisBloomFilter.mayExist("topic_read:8839540:20190609", "76930298",redisTemplate));
        System.out.println(redisBloomFilter.mayExist("topic_read:8839540:20190609", "76930246",redisTemplate));
        System.out.println(redisBloomFilter.mayExist("topic_read:8839540:20190609", "76930248",redisTemplate));


    }
}
