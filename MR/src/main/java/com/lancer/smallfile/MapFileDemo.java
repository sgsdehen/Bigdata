package com.lancer.smallfile;

import com.lancer.utils.HadoopConfigurationUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @Author lancer
 * @Date 2022/3/7 10:08 下午
 * @Description 在SequenceFile的基础上对key进行排序，分为两部分index和data
 */
public class MapFileDemo {

    static {
        System.setProperty("HADOOP_USER_NAME", "root");
    }

    private static final Configuration conf = HadoopConfigurationUtil.getConf();

    /**
     * @param inputDir  小文件所在目录
     * @param outputDir 包含index和data两个文件
     * @throws IOException
     */
    private static void write(Path inputDir, Path outputDir) throws IOException {

        // 判断输出目录是否存在
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        SequenceFile.Writer.Option[] writerOpts = new SequenceFile.Writer.Option[]{
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(Text.class)
        };

        MapFile.Writer writer = new MapFile.Writer(conf, outputDir, writerOpts);

        if (fs.isDirectory(inputDir)) {
            Text k = new Text();
            Text v = new Text();

            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(inputDir, true);
            while (locatedFileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
                String fileName = locatedFileStatus.getPath().getName();
                String line;
                StringBuilder sb = new StringBuilder();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(locatedFileStatus.getPath())));
                while (StringUtils.isNotEmpty(line = br.readLine())) {
                    sb.append(line).append("\n");
                }
                br.close();
                k.set(fileName);
                v.set(sb.toString());
                writer.append(k, v);
            }
        }
        writer.close();
    }

    /**
     * 读取MapFile
     *
     * @param inputDir 需要读取的SequenceFile的路径
     * @throws IOException
     */
    private static void read(Path inputDir) throws IOException {
        // 创建一个Reader实例
        MapFile.Reader reader = new MapFile.Reader(inputDir, conf);
        Text k = new Text();
        Text v = new Text();
        while (reader.next(k, v)) {
            System.out.printf("fileName：%s，context：%s", k, v);
        }
        reader.close();
    }

    public static void main(String[] args) throws IOException {
        write(new Path("/input"), new Path("/output"));
        read(new Path("/output"));
    }
}
