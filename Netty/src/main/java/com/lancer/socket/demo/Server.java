package com.lancer.socket.demo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author lancer
 * @Date 2022/3/23 1:00 下午
 * @Description Socket编程，BIO（Blocking IO）同步阻塞模型
 * 1. 每个请求都需要创建独立的线程，与对应的客户端进行数据Read，业务处理，数据Write
 * 2. 并发数较大时，需要创建大量线程来处理连接，系统资源占用较大
 * 3. 连接建立后，如果当前线程暂时没有数据可读，则线程就阻塞在Read操作上，造成线程资源浪费
 */
public class Server {
    public static void main(String[] args) throws IOException {
        // 1. 创建一个线程池，如果有客户端连接就创建一个线程，与之通信
        ExecutorService executorService = Executors.newCachedThreadPool();
        // 2. 创建ServerSocket对象
        ServerSocket serverSocket = new ServerSocket(9999);
        System.out.println("服务器已启动...");
        while (true) {
            // 3. 监听客户端
            final Socket socket = serverSocket.accept();
            System.out.println("有客户端连接");
            // 4. 开启新的线程处理
            executorService.execute(() -> handle(socket));
        }
    }

    private static void handle(Socket socket) {
        System.out.printf("线程ID：%d\t线程名称：%s\n", Thread.currentThread().getId(), Thread.currentThread().getName());
        try {
            // 从socket中取出输入流来接收消息
            InputStream is = socket.getInputStream();
            byte[] bytes = new byte[1024];
            int read = is.read(bytes);
            System.out.println("客户端发送过来的消息：" + new String(bytes, 0, read));

            // 从socket中取出输出流来返回消息
            OutputStream os = socket.getOutputStream();
            os.write("接收到您的消息了".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
