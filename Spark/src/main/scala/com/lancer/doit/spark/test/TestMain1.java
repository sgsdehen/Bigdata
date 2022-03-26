package com.lancer.doit.spark.test;

import redis.clients.jedis.Jedis;

import java.util.regex.Pattern;

/**
 * @Author lancer
 * @Date 2022/2/8 7:46 下午
 * @Description
 */
public class TestMain1 {
    public static void main(String[] args) {

        Jedis jedis = new Jedis("bigdata01", 6379);

        String abc = jedis.get("abc");
        System.out.println(abc);
    }
}
