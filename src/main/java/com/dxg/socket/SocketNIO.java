package com.dxg.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class SocketNIO {

    public static final int PORT = 8385;

    public static final List<SocketChannel> CLIENTS = new ArrayList<>();


    public static void main(String[] args) throws Exception {
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        // 设置服务端为非阻塞
        serverSocket.configureBlocking(false);
        serverSocket.bind(new InetSocketAddress(PORT));
        System.out.println("Server Start "+ PORT +" Listen...");
        while (true) {
            // 由于服务端设置连接为非阻塞，即就是在ACCEPT时，没有接收到新的链接会直接返回。测试机器，频繁调触发系统调用，我怕我机器顶不住
            TimeUnit.MILLISECONDS.sleep(500);
            SocketChannel client = serverSocket.accept();
            if (Objects.nonNull(client)) {
                // 获取到客户端链接，设置其数据的读取为非阻塞
                client.configureBlocking(false);
                CLIENTS.add(client);
                System.out.println("new client:" + client.getRemoteAddress());
            }

            // 无论客户端是否发送数据，每次都需要遍历全量的客户端连接，造成不必要的资源浪费。
            CLIENTS.parallelStream().forEach(chient -> {
                try {
                    // 分配堆外内存
                    ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(1024);

                    int num = chient.read(tmpBuffer);
                    // 不阻塞，一定会返回
                    if (0 < num) {
                        // 有数据
                        tmpBuffer.flip();
                        byte[] datas = new byte[tmpBuffer.limit()];
                        tmpBuffer.get(datas);
                        String str = new String(datas);

                        System.out.println(str);
                    } else if (0 == num) {
                        // 接收到空数据
                    } else {
                        // 小于0 客户端断开
                        tmpBuffer.clear();
                        System.out.println("client close:" + client.getRemoteAddress());
                        client.close();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
