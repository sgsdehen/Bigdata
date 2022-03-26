package org.thq.week01.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
/**
 * @Author lancer
 * @Date 2022/2/18 6:36 下午
 * @Description 新版本中继承UDF，实现一个或多个evaluate已经被废弃; 一般简单类型可以用UDF，复杂类型用GenericUDF
 */

public class MyStringLength extends GenericUDF {

    /**
     * 输入参数类型的鉴别器对象
     *
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 判断输入参数的个数
        if (objectInspectors.length != 1) {
            throw new UDFArgumentLengthException("Input Args Length Error!!!");
        }
        // 判断输入参数的类型
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            // 如果参数不是基本类型
            throw new UDFArgumentTypeException(0, "Input Args Type Error!!!");
        }
        // 函数本身返回值为Int类型，需要返回int类型的鉴别器对象
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的逻辑处理
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null) {
            return 0;
        }
        // 使用java求字符串长度的方法
        return deferredObjects[0].get().toString().length();
    }

    /**
     * 显示函数的用法
     * @param strings
     * @return
     */
    @Override
    public String getDisplayString(String[] strings) {
        return "Usage：MyStringLength(String str)";
    }
}
