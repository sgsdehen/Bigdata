package org.thq.week01.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

public class Add extends UDF {

    public  String evaluate(String s) throws UDFArgumentException {
        if(s == null) {
            throw new UDFArgumentException("string can't be null");
        }

        if(s.length() == 0) {
            return "";
        }
        return s + "666";
    }
}
