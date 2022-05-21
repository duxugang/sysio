package com.dxg.socket;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * NIO至多路复用器单线程版本
 */
public class SocketNIOSingleMultiplexer {

    public static final int PORT = 8385;

    public static Selector selector = null;

    public static void main(String[] args) throws Exception {
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.configureBlocking(false);
        serverSocket.bind(new InetSocketAddress(PORT));

        selector = Selector.open();

        serverSocket.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("Server Start " + PORT + " And Start Accept...");

        while (true) {
            int num = selector.select();
            if (0 < num) {
                // 这里只能使用单线程串行化处理。如果为多线程并行化处理，A客户端数据在Thread1还未处理完成，
                // Thread2，Thread3等线程也会继续处理，验证造成数据包无法解析。
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = keys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    if (key.isAcceptable()) {
                        acceptHander(key);
                    } else if (key.isReadable()) {
                        readHandler(key);
                    } else if (key.isWritable()) {
                        writeHandle(key);
                    }
                }

            }
        }

    }

    /**
     * 接收连接的处理器
     */
    public static void acceptHander(SelectionKey key) throws Exception {
        // 接收连接只能是ServerSocket
        ServerSocketChannel serverSocket = (ServerSocketChannel) key.channel();
        SocketChannel clientSocket = serverSocket.accept();
        // 拿到连接，设置其为非阻塞
        clientSocket.configureBlocking(false);

        // 定义当前Socket所使用的buffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
        // 将新接收的文件描述符，注册至多路复用器
        clientSocket.register(selector, SelectionKey.OP_READ, buffer);

        System.out.println("new client:" + clientSocket.getRemoteAddress());
    }

    /**
     * 数据读取处理器
     */
    public static void readHandler(SelectionKey key) throws Exception {
        // 客户端连接
        SocketChannel clientSocket = (SocketChannel) key.channel();
        // 获取buffer
        ByteBuffer buffer = (ByteBuffer) key.attachment();
        buffer.clear();
        while (true) {
            int num = clientSocket.read(buffer);
            if (0 < num) {
                // 将读取到的数据写回给客户端
                clientSocket.register(selector, SelectionKey.OP_WRITE, buffer);
            } else if (0 == num) {
                // 空数据
                break;
            } else {
                // 断开连接的处理
                System.out.println("client close:" + clientSocket.getRemoteAddress());
                key.cancel();
                break;
            }
        }
    }

    public static void writeHandle(SelectionKey key) throws Exception {
        SocketChannel clientSocket = (SocketChannel) key.channel();
        // 注册多路复用器时，将此Socket的buffer也附加上了并将获取到数据写入此buffer，在此处直接取出使用
        ByteBuffer buffer = (ByteBuffer) key.attachment();
        // 移动buffer前后两个指针，使得数据可以被写出
        buffer.flip();

        while (buffer.hasRemaining()) {
            clientSocket.write(buffer);
        }

        buffer.clear();
        // 写事件，关注的是数据能不能写，也就是内核中的Send-Q是否已满，而不是你要写。所以在写完需要移除掉写事件的监听
        key.cancel();
    }

}
