package com.lancer.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * @Author lancer
 * @Date 2022/3/7 11:08 下午
 * @Description
 */
public class HadoopFileSystemUtil {
    public static FileSystem getFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }
}
