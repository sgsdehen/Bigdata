package com.lancer.mr.join;

import com.lancer.utils.HadoopConfigurationUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * @Author lancer
 * @Date 2022/3/8 2:32 下午
 * @Description
 */
@Slf4j
public class ReduceJoinDriver {

    private static final Configuration conf = HadoopConfigurationUtil.getConf();

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class DeliverBean implements Writable {

        private String userId;
        private String positionId;
        private String date;
        private String positionName;
        // 判断是投递数据还是职位数据标识
        private String flag;

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.getUserId());
            dataOutput.writeUTF(this.getPositionId());
            dataOutput.writeUTF(this.getDate());
            dataOutput.writeUTF(this.getPositionName());
            dataOutput.writeUTF(this.getFlag());
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.setUserId(dataInput.readUTF());
            this.setPositionId(dataInput.readUTF());
            this.setDate(dataInput.readUTF());
            this.setPositionName(dataInput.readUTF());
            this.setFlag(dataInput.readUTF());
        }

        @Override
        public String toString() {
            return "{" +
                    "userId='" + userId + '\'' +
                    ", positionId='" + positionId + '\'' +
                    ", date='" + date + '\'' +
                    ", positionName='" + positionName + '\'' +
                    ", flag='" + flag + '\'' +
                    '}';
        }
    }

    private static class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, DeliverBean> {

        private static String fileName;

        private final Text k = new Text();
        private final DeliverBean deliverBean = new DeliverBean();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, DeliverBean>.Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DeliverBean>.Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\t");
            if ("position.txt".equals(fileName)) {
                deliverBean.setPositionId(splits[0]);
                deliverBean.setPositionName(splits[1]);
                deliverBean.setFlag("position");

                deliverBean.setUserId("");
                deliverBean.setDate("");
            } else if ("deliver_info.txt".equals(fileName)) {
                deliverBean.setUserId(splits[0]);
                deliverBean.setPositionId(splits[1]);
                deliverBean.setDate(splits[2]);
                deliverBean.setFlag("deliver");

                deliverBean.setPositionName("");
            }
            k.set(deliverBean.getPositionId());
            context.write(k, deliverBean);
        }
    }

    private static class ReduceJoinReducer extends Reducer<Text, DeliverBean, DeliverBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<DeliverBean> values, Reducer<Text, DeliverBean, DeliverBean, NullWritable>.Context context) throws IOException, InterruptedException {

            // 相同positionId的bean对象，放到一起（1个职位数据，n个投递行为数据）
            ArrayList<DeliverBean> deBeans = new ArrayList<>();
            DeliverBean deliverBean = new DeliverBean();

            // values中返回的bean，都是同一个对象，故需要深拷贝
            for (DeliverBean bean : values) {
                if ("deliver".equals(bean.getFlag())) {
                    DeliverBean newBean = new DeliverBean();
                    newBean.setUserId(bean.getUserId());
                    newBean.setPositionId(bean.getPositionId());
                    newBean.setPositionName(bean.getPositionName());
                    newBean.setDate(bean.getDate());
                    newBean.setFlag(bean.getFlag());
                    deBeans.add(newBean);
                } else {
                    // 职位
                    deliverBean.setUserId(bean.getUserId());
                    deliverBean.setPositionId(bean.getPositionId());
                    deliverBean.setPositionName(bean.getPositionName());
                    deliverBean.setDate(bean.getDate());
                    deliverBean.setFlag(bean.getFlag());
                }
            }

            // 遍历投递行为数据，拼接positionName
            for (DeliverBean bean : deBeans) {
                bean.setPositionName(deliverBean.getPositionName());
                context.write(bean, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf, "reduceJoin");

        job.setJarByClass(ReduceJoinDriver.class);

        job.setMapperClass(ReduceJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DeliverBean.class);

        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(DeliverBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("./data/deliver_info.txt"), new Path("./data/position.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
