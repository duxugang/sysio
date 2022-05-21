package com.dxg.disk;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DiskFileIo {

    /**
     * 输出目录
     */
    private static String PATH = "/home/sysio/";

    /**
     * 输入的数据
     */
    private static final byte[] DATA = "123456789\n".getBytes();

    /**
     * 执行写入的时间
     */
    private static final int EXECUTE_TIME = 3;

    /**
     * 控制写入的状态
     */
    private volatile static boolean FLAG = true;


    public static void main(String[] args) throws Exception {
        DiskFileIo diskFileIo = new DiskFileIo();
        switch (args[0]) {
            case "1":
                diskFileIo.baseWrite("out1.log");
                break;
            case "2":
                diskFileIo.bufferedWrite("out2.log");
                break;
            case "3":
                diskFileIo.randomAccessFileMmap("out3.log");
                break;
            case "4":
                diskFileIo.randomAccessFileMmap("out4.log");
                break;
        }

        ByteBuffer onHeap = ByteBuffer.allocate(1024);
        ByteBuffer offHeap = ByteBuffer.allocateDirect(1024);

    }


    /**
     * 基础文件流写入
     */
    public void baseWrite(String fileName) throws Exception {
        FileOutputStream outputStream =
                new FileOutputStream(new File(PATH + fileName));
        startMonit();
        while (FLAG) {
            outputStream.write(DATA);
        }
    }

    /**
     * buffered文件流写入
     *
     * @param fileName
     */
    public void bufferedWrite(String fileName) throws Exception {
        BufferedOutputStream outputStream =
                new BufferedOutputStream(new FileOutputStream(new File(PATH + fileName)));
        startMonit();
        while (FLAG) {
            outputStream.write(DATA);
        }
    }

    /**
     * NIO基础文件写入
     *
     * @param fileName
     * @throws Exception
     */
    public void randomAccessFileTest1(String fileName) throws Exception {
        RandomAccessFile accessFile = new RandomAccessFile(PATH + fileName, "rw");
        startMonit();
        while (FLAG) {
            accessFile.write(DATA);
        }
    }

    /**
     * NIO mmap文件写入
     *
     * @param fileName
     * @throws Exception
     */
    public void randomAccessFileMmap(String fileName) throws Exception {
        RandomAccessFile accessFile = new RandomAccessFile(PATH + fileName, "rw");
        FileChannel channel = accessFile.getChannel();
        // 在程序和内核中创建一个共享内存空间
        // 调用了内核的mmap
        MappedByteBuffer mmap =
                channel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
        startMonit();

        while (FLAG) {
            mmap.put(DATA);
        }
    }


    /**
     * 开启监控线程
     */
    public void startMonit() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            try {
                TimeUnit.SECONDS.sleep(EXECUTE_TIME);
                FLAG = false;
                executor.shutdownNow();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

}
