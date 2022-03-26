package com.lancer.socket.demo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * @Author lancer
 * @Date 2022/3/23 1:14 下午
 * @Description Socket编程
 */
public class Client {
    public static void main(String[] args) throws IOException {
        while (true) {
            // 1. 创建Socket对象
            Socket socket = new Socket("127.0.0.1", 9999);
            // 2. 从Socket中取出输出流，并发消息
            OutputStream os = socket.getOutputStream();
            System.out.print("请输入：");
            Scanner sc = new Scanner(System.in);
            String msg = sc.nextLine();
            os.write(msg.getBytes(StandardCharsets.UTF_8));
            // 3. 从Socket中取出输入流，接收响应
            InputStream is = socket.getInputStream();
            byte[] bytes = new byte[1024];
            int read = is.read(bytes);
            System.out.println("服务器响应：" + new String(bytes, 0, read).trim());
            // 4. 关闭
            socket.close();
        }
    }
}
