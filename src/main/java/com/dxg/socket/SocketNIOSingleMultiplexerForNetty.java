package com.dxg.socket;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SocketNIOSingleMultiplexerForNetty {

    public static void main(String[] args) {
        // 创建一个线程组，去管理所有线程 loop
        ThreadSelectorGroup threadSelectorGroup = new ThreadSelectorGroup(3,
                4);

        // 绑定端口
        threadSelectorGroup.bind(8381);
        threadSelectorGroup.bind(8382);
        threadSelectorGroup.bind(8383);
        threadSelectorGroup.bind(8384);
        threadSelectorGroup.bind(8385);
    }

}

/**
 * 管理线程
 * 分配fd
 */
class ThreadSelectorGroup {

    public static final String BOSS = "boss";
    public static final String WORKER = "worker";

    /**
     * 分配线程索引
     */
    private AtomicInteger nowBossIndex = new AtomicInteger(0);
    private AtomicInteger nowWorkerIndex = new AtomicInteger(0);

    /**
     * 线程集合
     */
    private SelectorThread[] bossSelectorThread = null;
    private SelectorThread[] workerSelectorThread = null;


    private ServerSocketChannel serverSocketChannel = null;

    public ThreadSelectorGroup(int bossThread, int workerThread) {
        int boss = bossThread <= 0 ? 1 : bossThread;
        int worker = workerThread <= 0 ? 1 : workerThread;
        this.bossSelectorThread = new SelectorThread[boss];
        this.workerSelectorThread = new SelectorThread[worker];

        ExecutorService bossThreadPool = Executors.newFixedThreadPool(boss,
                this.getThreadFactory(BOSS));
        ExecutorService workerThreadPool = Executors.newFixedThreadPool(worker,
                this.getThreadFactory(WORKER));

        // 初始化接收连接线程
        for (int i = 0; i < boss; i++) {
            this.bossSelectorThread[i] = new SelectorThread(this);
            bossThreadPool.execute(this.bossSelectorThread[i]);
        }
        // 初始化处理消息线程
        for (int i = 0; i < worker; i++) {
            this.workerSelectorThread[i] = new SelectorThread(this);
            workerThreadPool.execute(this.workerSelectorThread[i]);
        }
    }


    /**
     * 绑定端口
     *
     * @param prot
     */
    public void bind(int prot) {
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.bind(new InetSocketAddress(prot));

            this.distribute(this.serverSocketChannel);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 将fd关联至对应Selector线程进行自主注册
     */
    public void distribute(Channel channel) {
        SelectorThread selectorThread = this.next(channel);
        try {
            // 需要先将待注册的channel加入队列，待线程处理完成后再唤醒selector
            selectorThread.getBlockingQueue().put(channel);
            selectorThread.getSelector().wakeup();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 轮询获取需要注册到的selector
     *
     * @param channel
     * @return
     */
    private SelectorThread next(Channel channel) {
        SelectorThread selectorThread = null;
        if (channel instanceof ServerSocketChannel) {
            selectorThread = this.bossSelectorThread[
                    this.nowBossIndex.getAndIncrement() %
                            this.bossSelectorThread.length];
        }
        if (channel instanceof SocketChannel) {
            selectorThread = this.workerSelectorThread[
                    this.nowWorkerIndex.getAndIncrement() %
                            this.workerSelectorThread.length];
        }
        return selectorThread;
    }


    /**
     * 获取线程工厂
     *
     * @param type
     * @return
     */
    private ThreadFactory getThreadFactory(String type) {
        ThreadFactory threadFactory = null;
        switch (type) {
            case BOSS:
                threadFactory =
                        new ThreadFactoryBuilder().setNameFormat(BOSS +
                                "-pool-%d").build();
                break;
            case WORKER:
                threadFactory =
                        new ThreadFactoryBuilder().setNameFormat(WORKER +
                                "-pool-%d").build();
                break;
        }
        return threadFactory;
    }

}

/**
 * 一个线程独立占有一个多路复用器
 */
@Getter
class SelectorThread extends ThreadLocal<LinkedBlockingQueue<Channel>> implements Runnable {

    private Selector selector = null;

    /**
     * 每个线程需要持有线程组的引用，线程组中维护连接的分配规则
     */
    private ThreadSelectorGroup threadSelectorGroup;

    private BlockingQueue<Channel> blockingQueue = get();

    public SelectorThread(ThreadSelectorGroup threadSelectorGroup) {
        // 初始化多路复用器
        try {
            this.selector = Selector.open();
            this.threadSelectorGroup = threadSelectorGroup;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        System.out.println("已启动的线程名称：" + Thread.currentThread().getName());
        while (true) {
            try {
                int n = this.selector.select();
                if (n > 0) {
                    Set<SelectionKey> keys = this.selector.selectedKeys();
                    Iterator<SelectionKey> iterator = keys.iterator();
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        iterator.remove();
                        if (key.isAcceptable()) {
                            // 接收连接
                            this.acceptHandler(key);
                        } else if (key.isReadable()) {
                            // 读取数据
                            this.readHandler(key);
                        } else if (key.isWritable()) {
                            // 写出数据
                            this.writeHandler(key);
                        }
                    }
                } else if (n == 0) {
                    // 程序启动，线程阻塞至 select 当被唤醒时，会接收空数据的事件。
                    // 可以在这里获取队列中的数据进行注册处理
                    this.registerHandle();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 注册
     */
    private void registerHandle() {
        if (this.blockingQueue.isEmpty()) {
            return;
        }
        try {
            Channel channel = this.blockingQueue.take();
            // 服务端关注的是接收事件
            if (channel instanceof ServerSocketChannel) {
                ServerSocketChannel serverSocketChannel = (ServerSocketChannel) channel;
                serverSocketChannel.register(this.selector,
                        SelectionKey.OP_ACCEPT);
                System.out.println("服务端注册-线程名称：" + Thread.currentThread().getName());
            }
            // 客户端关注的是读取事件
            else if (channel instanceof SocketChannel) {
                SocketChannel socketChannel = (SocketChannel) channel;

                ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
                socketChannel.register(this.selector, SelectionKey.OP_READ,
                        buffer);
                System.out.println("线程名称：" + Thread.currentThread().getName() +
                        ",接收到客户端：" + socketChannel.getRemoteAddress());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 接收连接
     */
    private void acceptHandler(SelectionKey key) {

        try {
            ServerSocketChannel channel = (ServerSocketChannel) key.channel();
            SocketChannel client = channel.accept();
            client.configureBlocking(false);

            // 将接收到的连接分配至相应的Selector中
            threadSelectorGroup.distribute(client);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 读取数据
     */
    private void readHandler(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();
        buffer.clear();
        while (true) {
            try {
                int n = socketChannel.read(buffer);

                if (n > 0) {
                    // 将读取到的数据输出至控制台
                    buffer.flip();
                    byte[] datas = new byte[buffer.limit()];
                    buffer.get(datas);
                    String str = new String(datas);
                    System.out.println("线程名称：" + Thread.currentThread().getName() + ",内容：" + str);

                    // 将读取到的数据写回给客户端
                    socketChannel.register(this.selector, SelectionKey.OP_WRITE,
                            buffer);
                } else if (n == 0) {
                    // 空数据
                    break;
                } else {
                    // 断开连接的处理
                    System.out.println("client close:" + socketChannel.getRemoteAddress());
                    key.cancel();
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 写数据
     *
     * @param key
     */
    private void writeHandler(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();
        buffer.flip();
        while (buffer.hasRemaining()) {
            try {
                socketChannel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            buffer.clear();
            // 多路复用器针对单个fd注册一个事件读/写/接收/连接
            // 在读时注册的读事件被被写事件覆盖，在此处重新注册
            socketChannel.register(this.selector, SelectionKey.OP_READ,
                    buffer);
        } catch (ClosedChannelException e) {
            e.printStackTrace();
        }
    }


    @Override
    protected LinkedBlockingQueue<Channel> initialValue() {
        return new LinkedBlockingQueue<>();
    }
}


