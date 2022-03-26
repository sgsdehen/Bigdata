package com.lancer.nio.selector;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;

/**
 * @Author lancer
 * @Date 2022/3/23 11:36 下午
 * @Description
 */
public class SelectorDemo {
    public static void main(String[] args) throws IOException {
        // 1. 打开一个服务端通道
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();

        // 2. 绑定对应的端口号
        serverSocketChannel.bind(new InetSocketAddress(9999));

        // 3. 通道默认是阻塞的，需要设置为非阻塞
        serverSocketChannel.configureBlocking(false);

        // 4. 创建选择器
        Selector selector = Selector.open();

        // 5. 将服务端通道注册到选择器上，并指定注册监听的事件为OP_ACCEPT
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("服务端启动成功");

        while (true) {
            // 6. 检查选择器是否有事件, 阻塞10s, 返回值是事件个数
            int select = selector.select(10000);
            // System.out.println("select : " + select);
            if (select == 0) {
                System.out.println("没有事件发生");
            } else {
                // 7. 获取事件集合
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    // 8. 判断事件是否是客户端连接事件SelectionKey.isAcceptable()
                    if (key.isAcceptable()) {
                        // 9. 得到客户端通道，并将通道注册到选择器上，并指定监听事件为OP_READ
                        try {
                            SocketChannel socketChannel = serverSocketChannel.accept();
                            System.out.println("------有客户端连接");
                            // 通道必须设置成非阻塞的状态，因为Selector需要轮询监听每个通道的事件
                            socketChannel.configureBlocking(false);
                            socketChannel.register(selector, SelectionKey.OP_READ);

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    // 10. 判断是否是客户端就绪事件SelectionKey.isReadable()
                    if (key.isReadable()) {
                        // 11. 得到客户端通道，读取数据到缓冲区
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        ByteBuffer allocate = ByteBuffer.allocate(1024);
                        try {
                            int read = socketChannel.read(allocate);
                            if (read > 0) {
                                System.out.println("客户端消息 ：" + new String(allocate.array(), 0, read, StandardCharsets.UTF_8));
                                // 12. 给客户端写回数据
                                socketChannel.write(ByteBuffer.wrap("服务器收到客户端消息".getBytes(StandardCharsets.UTF_8)));
                                socketChannel.close();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    // 13. 从集合中删除对应的事件，因为防止二次处理
                    iterator.remove();
                }
            }
        }
    }
}
