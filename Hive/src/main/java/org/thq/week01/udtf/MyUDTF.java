package org.thq.week01.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MyUDTF extends GenericUDTF {

    private ArrayList<String> outList = new ArrayList<>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 定义输出数据的列名和类型
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOI = new ArrayList<>();

        // 添加输出数据的列名和类型
        fieldNames.add("lineToWord");
        fieldOI.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOI);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // 获取原始数据
        String arg = objects[0].toString();

        // 获取数据传入的第二个参数，此处为分隔符
        String splitKey = objects[1].toString();

        // 将原始数据按照传入的分隔符进行切分
        String[] fields = arg.split(splitKey);

        // 遍历切分后的结果
        for (String field : fields) {
            // 集合为复用的，先清空集合
            outList.clear();

            // 将每一个单词添加至集合
            outList.add(field);

            //将集合内容写出（此处添加一个输出一个）
            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
