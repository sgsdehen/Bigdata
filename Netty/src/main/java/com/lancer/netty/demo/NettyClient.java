package com.lancer.netty.demo;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;

/**
 * @Author lancer
 * @Date 2022/3/23 1:17 上午
 * @Description Netty客户端
 */
public class NettyClient {

    /**
     * 自定义处理Handler,实现ChannelInboundHandler接口，从而实现消息入站
     */
    private static class NettyClientHandler implements ChannelInboundHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        @Override
        public void channelUnregistered(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        /**
         * channel就绪事件
         * @param channelHandlerContext
         * @throws Exception
         */
        @Override
        public void channelActive(ChannelHandlerContext channelHandlerContext) throws Exception {
            channelHandlerContext.writeAndFlush(Unpooled.copiedBuffer("我是netty客户端", CharsetUtil.UTF_8));
        }

        @Override
        public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        /**
         * channel读就绪事件
         * @param channelHandlerContext
         * @param msg
         * @throws Exception
         */
        @Override
        public void channelRead(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
            ByteBuf byteBuf = (ByteBuf) msg;
            System.out.println("服务端返回的消息：" + byteBuf.toString(CharsetUtil.UTF_8));
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        @Override
        public void userEventTriggered(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {

        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        @Override
        public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {

        }

        @Override
        public void handlerAdded(ChannelHandlerContext channelHandlerContext) throws Exception {

        }

        @Override
        public void handlerRemoved(ChannelHandlerContext channelHandlerContext) throws Exception {

        }
    }

    public static void main(String[] args) throws InterruptedException {
        // TODO 1. 创建线程组
        EventLoopGroup loopGroup = new NioEventLoopGroup();

        // TODO 2. 创建客户端启动助手
        Bootstrap bootstrap = new Bootstrap();

        // TODO 3. 设置线程组
        bootstrap.group(loopGroup)

                // TODO 4. 设置客户端channel实现为NIO
                .channel(NioSocketChannel.class)

                // TODO 5. 创建一个channel初始化对象
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        // TODO 6. 向pipeline中添加自定义业务处理handler
                        socketChannel.pipeline().addLast(new NettyClientHandler());
                    }
                });
        // TODO 7. 启动客户端，等待连接服务端，同时将异步改成同步
        ChannelFuture future = bootstrap.connect("127.0.0.1", 9999).sync();

        // TODO 8. 关闭channel和关闭连接池
        future.channel().closeFuture().sync();
        loopGroup.shutdownGracefully();

    }
}
