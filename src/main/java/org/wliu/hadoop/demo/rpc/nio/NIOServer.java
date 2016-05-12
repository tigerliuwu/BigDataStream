package org.wliu.hadoop.demo.rpc.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class NIOServer {

	// 通道管理器
	private Selector selector;

	/**
	 * 获取一个serverSocket通道，并对该通道做一些初始化的工作
	 * 
	 * @param port
	 *            绑定到服务器的端口号
	 * @throws IOException
	 */
	public void initServer(int port) throws IOException {
		// 获取一个ServerSocket通道
		ServerSocketChannel serverChannel = ServerSocketChannel.open();
		// configure the channel as non-blocking
		serverChannel.configureBlocking(false);
		// bind the serverSocket of the channel to port
		serverChannel.socket().bind(new InetSocketAddress(port));
		// get a channel selector
		this.selector = Selector.open();
		// 将selector和channel绑定，并将该channel注册为OP_ACCEPT事件，注册事件后，当该事件发生时，selector.select()会返回，如果该事件没到达
		// selector.select()会一直阻塞。
		serverChannel.register(selector, SelectionKey.OP_ACCEPT);
	}

	/**
	 * 
	 */
	public void listen() throws IOException {
		System.out.println("the server has started.");
		// 轮洵访问selector
		while (true) {
			// 当注册的事件到达时，方法返回；否则，该方法会一直阻塞
			selector.select();
			Iterator<SelectionKey> iter = this.selector.selectedKeys().iterator();
			while (iter.hasNext()) {
				SelectionKey key = iter.next();
				iter.remove();
				// 客户端请求连接事件
				if (key.isAcceptable()) {
					ServerSocketChannel server = (ServerSocketChannel) key.channel();
					// 获取客户端连接的通道
					SocketChannel channel = server.accept();
					// 设置成非阻塞
					channel.configureBlocking(false);

					// 在这里可以给客户端发送消息
					channel.write(ByteBuffer.wrap(new String("send a message to the client...").getBytes()));
					// 为了可以接受客户端的信息，需要给channel注册读的权限
					channel.register(this.selector, SelectionKey.OP_READ);
				} else if (key.isReadable()) {
					read(key);
				}
			}
		}
	}

	public void read(SelectionKey key) throws IOException {
		SocketChannel channel = (SocketChannel) key.channel();
		ByteBuffer buffer = ByteBuffer.allocate(80);
		channel.read(buffer);
		String msg = new String(buffer.array()).trim();
		System.out.println("the server receive the message:" + msg);
		ByteBuffer outBuffer = ByteBuffer.wrap(msg.getBytes());
		channel.write(outBuffer);
	}

	public static void main(String[] args) throws IOException {
		NIOServer server = new NIOServer();
		server.initServer(8000);
		server.listen();
	}

}
