package com.lagou.mr.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private Double price;

    public OrderBean() {
    }

    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    // 指定排序规则
    // 先按照订单id比较
    // 再按照金额比较
    // 按照订单升序，按照金额降序
    @Override
    public int compareTo(OrderBean o) {
        int res = this.orderId.compareTo(o.getOrderId());
        if(res == 0) { // 订单id相同
            // 比较金额
            res = -this.price.compareTo(o.getPrice());
        }
        return res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return orderId +
                "\t" + price;
    }
}
