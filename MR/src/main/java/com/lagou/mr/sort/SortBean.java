package com.lagou.mr.sort;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 作为map输出的key，所以要实现Writable+comparable接口
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class SortBean implements WritableComparable<SortBean> {

    private Long selfDuration;
    private Long thirdPartDuration;
    private String deviceId;
    private Long sumDuration;


    // 指定排序规则
    // 希望按照总时长进行排序
    @Override
    public int compareTo(SortBean o) { // 返回值：0(相等)、1、-1
        // -1表示保持当前位置状态，不改变
        // 1表示交换位置

        // 改变1和-1的位置，可以改变降序或升序
        // 现在是降序
        if (this.sumDuration > o.sumDuration) {
            return -1;
        } else if (this.sumDuration < o.sumDuration) {
            return 1;
        } else {
            // 可以加入第二个判断条件，成为二次排序
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(selfDuration);
        dataOutput.writeLong(thirdPartDuration);
        dataOutput.writeUTF(deviceId);
        dataOutput.writeLong(sumDuration);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.selfDuration = dataInput.readLong();
        this.thirdPartDuration = dataInput.readLong();
        this.deviceId = dataInput.readUTF();
        this.sumDuration = dataInput.readLong();
    }
}
