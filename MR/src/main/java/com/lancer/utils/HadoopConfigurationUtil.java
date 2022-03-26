package com.lancer.utils;

import org.apache.hadoop.conf.Configuration;

/**
 * @Author lancer
 * @Date 2022/3/7 11:02 下午
 * @Description
 */
public class HadoopConfigurationUtil {

    public static Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");
        return conf;
    }
}
