package com.dxg.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;

public class SocketBIO {

    /**
     * 服务器监听的端口
     */
    public static final int PORT = 8385;

    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("Server Start " + PORT);

        // 阻塞  验证socket是内核级别的
        System.in.read();

        while (true) {
            // 阻塞1
            System.out.println("Prepare Accept Client");
            Socket socket = serverSocket.accept();
            System.out.println("接入新连接：" + socket.getPort());

            // 抛出一个新的线程去处理socket的读写
            new Thread(() -> {
                InputStream inputStream = null;
                BufferedReader reader = null;
                try {
                    inputStream = socket.getInputStream();
                    reader = new BufferedReader(new InputStreamReader(inputStream));
                    while (true) {
                        // 阻塞2
                        String data = reader.readLine();
                        if (Objects.nonNull(data)) {
                            System.out.println("Server rev：" + data);
                        } else {
                            socket.close();
                            System.out.println("Client Close");
                            return;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (Objects.nonNull(reader)) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    if (Objects.nonNull(inputStream)) {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }).start();
        }
    }
}
