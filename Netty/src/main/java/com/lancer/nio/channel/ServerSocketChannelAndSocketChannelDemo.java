package com.lancer.nio.channel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @Author lancer
 * @Date 2022/3/23 10:42 下午
 * @Description
 */
public class ServerSocketChannelAndSocketChannelDemo {
    private static class ServerSocketChannelDemo {
        public static void main(String[] args) throws IOException, InterruptedException {
            // 1. 打开一个服务端通道
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();

            // 2. 绑定对应的端口号
            serverSocketChannel.bind(new InetSocketAddress(9999));

            // 3. 通道默认是阻塞的，需要设置为非阻塞
            serverSocketChannel.configureBlocking(false);
            System.out.println("服务端启动成功");

            while (true) {
                // 4. 检查是否有客户端连接，有客户端连接会返回对应的通道
                SocketChannel socketChannel = serverSocketChannel.accept();
                if (Objects.isNull(socketChannel)) {
                    System.out.println("没有客户端连接。。。我去做别的事情");
                    Thread.sleep(3000);
                } else {
                    // 5. 获取客户端传递过来的数据，并把数据放在byteBuffer这个缓冲区中
                    ByteBuffer allocate = ByteBuffer.allocate(1024);
                    // read > 0:表示本次读到的有效字节数；read = 0:表示本次没有读到数据；read = -1:表示读到末尾
                    int read = socketChannel.read(allocate);
                    System.out.println("客户端发送过来的消息：" + new String(allocate.array(), 0, read, StandardCharsets.UTF_8));
                    // 6. 给客户端回写数据
                    socketChannel.write(ByteBuffer.wrap("服务端已收到消息".getBytes(StandardCharsets.UTF_8)));
                    // 7. 释放资源
                    socketChannel.close();
                }
            }
        }
    }

    private static class SocketChannelDemo {
        public static void main(String[] args) throws IOException {
            // 1. 打开通道, 设置连接IP和端口号
            SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress("127.0.0.1", 9999));

            // 2. 写出数据
            socketChannel.write(ByteBuffer.wrap("客户端发送消息".getBytes(StandardCharsets.UTF_8)));

            // 3. 读取服务端写回的数据
            ByteBuffer allocate = ByteBuffer.allocate(1024);
            int read = socketChannel.read(allocate);
            System.out.println("服务端消息：" + new String(allocate.array(), 0, read, StandardCharsets.UTF_8));

            // 4. 关闭通道
            socketChannel.close();
        }
    }
}
