package com.lagou.mr.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// 在reduce方法调用前已完成
public class CustomGroupingComparator extends WritableComparator {


    public CustomGroupingComparator() {
        super(OrderBean.class, true); // 注册自定义的GroupingComparator接受OrderBean对象
    }


    // 重写其中的compare方法，通过这个方法来让mr接受orderId相等，则两个对象相等的规则，key相等
    @Override
    public int compare(WritableComparable a, WritableComparable b) { // a和b就是OrderBean的对象
        // 比较两个对象的orderId
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
