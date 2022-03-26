package com.lancer.netty.demo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.CharsetUtil;

/**
 * @Author lancer
 * @Date 2022/3/23 12:34 上午
 * @Description Netty服务端
 */
public class NettyServer {

    /**
     * 自定义处理Handler,实现ChannelInboundHandler接口，从而实现消息入站
     */
    private static class NettyServerHandler implements ChannelInboundHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        @Override
        public void channelUnregistered(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        @Override
        public void channelActive(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        @Override
        public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        /**
         * channel读取事件
         * @param channelHandlerContext
         * @param msg 客户端发送给服务端的消息
         * @throws Exception
         */
        @Override
        public void channelRead(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
            ByteBuf byteBuf = (ByteBuf) msg;
            System.out.println("客户端发送过来的消息为：" + byteBuf.toString(CharsetUtil.UTF_8));
        }

        /**
         * channel读取完毕事件
         * @param channelHandlerContext
         * @throws Exception
         */
        @Override
        public void channelReadComplete(ChannelHandlerContext channelHandlerContext) throws Exception {
            // 消息出站
            channelHandlerContext.writeAndFlush(Unpooled.copiedBuffer("你好，我是netty服务端".getBytes(CharsetUtil.UTF_8)));
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {

        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        @Override
        public void handlerAdded(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        @Override
        public void handlerRemoved(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        /**
         * channel异常事件
         * @param channelHandlerContext
         * @param throwable
         * @throws Exception
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {
            throwable.printStackTrace();
            channelHandlerContext.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // TODO 1. 创建bossGroup线程组：处理网络事件 -- 连接事件
        // 默认线程个数为处理器线程数 * 2，1个就ok了
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);

        // TODO 2. 创建workerGroup线程组：处理网络事件 -- 读写事件
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        // TODO 3. 创建服务端启动助手
        ServerBootstrap serverBootstrap = new ServerBootstrap();

        // TODO 4. 设置bossGroup线程组和workerGroup线程组
        serverBootstrap.group(bossGroup, workerGroup)

                // TODO 5. 设置服务端Channel实现为NIO
                .channel(NioServerSocketChannel.class)

                // TODO 6. 参数设置
                // 设置等待连接的队列
                .option(ChannelOption.SO_BACKLOG, 128)
                // 设置channel活跃状态
                .childOption(ChannelOption.SO_KEEPALIVE, Boolean.TRUE)

                // TODO 7. 创建一个Channel初始化对象
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        // TODO 8. 向pipeline中添加自定义业务处理handler
                        channel.pipeline().addLast(new NettyServerHandler());
                    }
                });


        // TODO 9. 启动服务端并绑定端口，同时将异步改成同步
        ChannelFuture future = serverBootstrap.bind(9999).sync();

        // TODO 10. 关闭Channel(并不是真正意义上的关闭，而是监听Channel关闭的状态)和关闭连接池
        // 因为异步改成了同步的，只有触发了关闭，程序才会往下继续执行，从而关闭连接池
        future.channel().closeFuture().sync();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
