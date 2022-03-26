package com.lancer.smallfile;

import com.lancer.utils.HadoopConfigurationUtil;
import com.lancer.utils.HadoopFileSystemUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @Author lancer
 * @Date 2022/3/7 10:08 下午
 * @Description 将小文件合并成一个大的SequenceFile
 */
@Slf4j
public class SequenceFileDemo {
    static {
        System.setProperty("HADOOP_USER_NAME", "root");
    }

    private static final Configuration conf = HadoopConfigurationUtil.getConf();

    /**
     * @param inputDir   本地文件路径
     * @param outputFile HDFS文件
     * @throws IOException
     */
    private static void write(Path inputDir, Path outputFile) throws IOException {

        SequenceFile.Writer.Option[] writerOpts = new SequenceFile.Writer.Option[]{
                SequenceFile.Writer.file(outputFile),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class)
        };

        // 创建一个Writer实例
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, writerOpts);

        FileSystem fs = HadoopFileSystemUtil.getFileSystem(conf);
        if (fs.isDirectory(inputDir)) {
            Text k = new Text();
            Text v = new Text();

            FileStatus[] fileStatuses = fs.listStatus(inputDir);
            for (FileStatus fileStatus : fileStatuses) {
                String fileName = fileStatus.getPath().getName();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
                String line;
                StringBuilder sb = new StringBuilder();
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
     * 读取SequenceFile
     *
     * @param inputFile 需要读取的SequenceFile的路径
     * @throws IOException
     */
    private static void read(Path inputFile) throws IOException {
        SequenceFile.Reader.Option[] readerOpts = new SequenceFile.Reader.Option[]{
                SequenceFile.Reader.file(inputFile)
        };
        // 创建一个Reader实例
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, readerOpts);
        Text k = new Text();
        Text v = new Text();
        while (reader.next(k, v)) {
            System.out.printf("fileName：%s，context：%s", k, v);
        }
        reader.close();
    }

    public static void main(String[] args) throws IOException {
        write(new Path("/input"), new Path("/output/a"));
        read(new Path("/output/a"));
    }
}
