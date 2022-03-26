package com.lancer.nio.bytebuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @Author lancer
 * @Date 2022/3/23 3:53 下午
 * @Description Buffer获取会越界
 */
public class ByteBufferDemo {
    public static void main(String[] args) {
        getDataFromByteBuffer();
    }

    private static void createByteBuffer() {
        // 创建指定长度的Buffer
        ByteBuffer allocate = ByteBuffer.allocate(5);
        for (int i = 0; i < allocate.capacity(); i++) {
            System.out.println(allocate.get(i));
        }

        // 创建一个有内容的Buffer
        ByteBuffer wrap = ByteBuffer.wrap("lancer".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < wrap.capacity(); i++) {
            System.out.println(wrap.get());
        }
    }


    private static void addData2ByteBuffer() {
        ByteBuffer allocate = ByteBuffer.allocate(10);
        System.out.println("当前索引位置: " + allocate.position());
        System.out.println("最多能操作到哪个索引位置: " + allocate.limit());
        System.out.println("缓冲区长度: " + allocate.capacity());
        System.out.println("还有多少个可以操作的数据: " + allocate.remaining());

        System.out.println("========================");

        // 添加一个字节
        allocate.put((byte) 97);
        System.out.println("当前索引位置: " + allocate.position());
        System.out.println("最多能操作到哪个索引位置: " + allocate.limit());
        System.out.println("缓冲区长度: " + allocate.capacity());
        System.out.println("还有多少个可以操作的数据: " + allocate.remaining());

        System.out.println("========================");

        // 添加一个字节数组
        allocate.put("abc".getBytes(StandardCharsets.UTF_8));
        System.out.println("当前索引位置: " + allocate.position());
        System.out.println("最多能操作到哪个索引位置: " + allocate.limit());
        System.out.println("缓冲区长度: " + allocate.capacity());
        System.out.println("还有多少个可以操作的数据: " + allocate.remaining());

        System.out.println("========================");

        // 修改当前索引位置，覆盖写
        allocate.position(2);
        // 修改最多能操作到哪个索引的位置
        allocate.limit(6);
        System.out.println("当前索引位置: " + allocate.position());
        System.out.println("最多能操作到哪个索引位置: " + allocate.limit());
        System.out.println("缓冲区长度: " + allocate.capacity());
        System.out.println("还有多少个可以操作的数据: " + allocate.remaining());
    }

    private static void getDataFromByteBuffer() {
        ByteBuffer allocate = ByteBuffer.allocate(10);
        allocate.put("abcd".getBytes(StandardCharsets.UTF_8));
        System.out.println("当前索引位置: " + allocate.position());
        System.out.println("最多能操作到哪个索引位置: " + allocate.limit());
        System.out.println("缓冲区长度: " + allocate.capacity());
        System.out.println("还有多少个可以操作的数据: " + allocate.remaining());

        System.out.println("===================================");

        // 切换成读模式, limit = position;position = 0;
        allocate.flip();
        for (int i = 0; i < allocate.limit(); i++) {
            // 获取当前position的数据，改变position下标
            System.out.println(allocate.get());
            // 获取指定下标的数据，不改变position下标
            // System.out.println(allocate.get(i));
        }

        System.out.println("===================================");

        // position从0开始
        allocate.rewind();

        // 读取数据到字节数组中
        byte[] bytes = new byte[4];
        allocate.get(bytes);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));

        System.out.println("===================================");

        // 将缓冲区转化成字节数据返回
        System.out.println(new String(allocate.array(), StandardCharsets.UTF_8));

        System.out.println("===================================");

        // 切换成写模式，position = 0; limit = capacity;
        allocate.clear();
    }
}
