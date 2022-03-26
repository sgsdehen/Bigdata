package com.lancer.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @Author lancer
 * @Date 2022/1/10 6:41 下午
 * @Description 提取日志中的timestamp，放入日志header中
 */
public class EventTimeStampInterceptor implements Interceptor {

    private final String fieldName;
    private final String headName;

    public EventTimeStampInterceptor(String fieldName, String headName) {
        this.fieldName = fieldName;
        this.headName = headName;
    }

    @Override
    public void initialize() {

    }

    /**
     * 从日志数据中，提取时间戳
     * @param event 传入一条Event
     * @return 返回处理后的结果
     */
    @Override
    public Event intercept(Event event) {
        //
        byte[] body = event.getBody();
        String json = new String(body);
        JSONObject jsonObject = JSON.parseObject(json);
        Long fieldName = jsonObject.getLong(this.fieldName);
        Map<String, String> headers = event.getHeaders();
        headers.put(this.headName, String.valueOf(fieldName));
        event.setHeaders(headers);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    /**
     * 通过build()方法构造拦截器对象。
     * 调用静态内部类：com.lancer.flume.interceptor.EventTimeStampInterceptor$EventTimeStampInterceptorBuilder
     */
    public static class EventTimeStampInterceptorBuilder implements Builder {

        private String fieldName;
        private String headerName;

        @Override
        public Interceptor build() {
            return new EventTimeStampInterceptor(fieldName, headerName);
        }

        // 获取配置文件中的参数
        @Override
        public void configure(Context context) {
            this.fieldName = context.getString("fieldname");
            this.headerName = context.getString("headername");
        }
    }
}
