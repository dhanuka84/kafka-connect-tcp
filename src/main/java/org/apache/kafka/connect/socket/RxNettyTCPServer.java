package org.apache.kafka.connect.socket;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.channel.ConnectionHandler;
import io.reactivex.netty.channel.ObservableConnection;
import io.reactivex.netty.server.RxServer;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

public final class RxNettyTCPServer implements Runnable{
	
	private static final Logger log = LoggerFactory.getLogger(RxNettyTCPServer.class);
	
	protected static final ConcurrentLinkedQueue<byte[]> messages = new ConcurrentLinkedQueue<>();
	private RxServer<ByteBuf, ByteBuf> nettyServer;

	static final int DEFAULT_PORT = 8791;

	private final int port;
	

	public RxNettyTCPServer(int port) {
		this.port = port;
	}

	public RxServer<ByteBuf, ByteBuf> createServer() {
		RxServer<ByteBuf, ByteBuf> server = RxNetty
				.newTcpServerBuilder(port, new ConnectionHandler<ByteBuf, ByteBuf>() {
					@Override
					public Observable<Void> handle(final ObservableConnection<ByteBuf, ByteBuf> connection) {

						final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
						final AtomicInteger fullLenth = new AtomicInteger();
						final AtomicInteger count = new AtomicInteger();

						System.out.println("New client connection established. " + Thread.currentThread().getId());
						connection.writeBytesAndFlush("Welcome! \n\n".getBytes());


						Observable<Void> response = connection.getInput().flatMap(new Func1<ByteBuf, Observable<Void>>() {

							@Override
							public Observable<Void> call(ByteBuf originalBuff) {
								
								count.incrementAndGet();

								System.out.println(" Max Capacity: " + originalBuff.maxCapacity());
								ByteBuf dupBuff = originalBuff.duplicate();

								System.out.println(" Capacity: " + dupBuff.capacity());
								byte[] bytes;
								int offset;
								int length = dupBuff.readableBytes();

								if (dupBuff.hasArray()) {
									bytes = dupBuff.array();
									offset = dupBuff.arrayOffset();
								} else {
									bytes = new byte[length];
									dupBuff.getBytes(dupBuff.readerIndex(), bytes);
									offset = 0;
									System.out.println("doesn't has array");
								}
								
								fullLenth.addAndGet(length);

								try {
									outputStream.write(bytes);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}

								originalBuff.clear();
								dupBuff.clear();

								System.out.println("onNext: " + length + " readable : " + dupBuff.isReadable()
										+ "  threadId" + Thread.currentThread().getId());

								if (bytes.length > 0) {
									return connection.writeBytesAndFlush(bytes);
								} else {
									System.out.println("Msg Empty: " + bytes.length);
									return Observable.empty();
								}
							}
						}).subscribeOn(Schedulers.io())
						  .doAfterTerminate(new Action0() {
							@Override
							public void call() {
								// connection.close(true);
								byte finalBytes[] = outputStream.toByteArray();
								messages.add(finalBytes);
								//createFile(finalBytes);
								try {
									outputStream.flush();
									outputStream.close();
									
									System.out.println(fullLenth+" --- Connection Closed --- " + finalBytes.length);
									System.out.println(count);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}finally{
									count.set(0);
									fullLenth.set(0);
								}
								
								
							}
						});

						// subscribePrint(response, "Test");

						return response;
					}
				})
				
				.build();
		return server;
	}

	/*public static void main(final String[] args) {
		new RxNettyTCPServer(DEFAULT_PORT).createServer().startAndWait();
	}
*/

	
	public static void createFile(byte[] content){
		FileOutputStream fos;
		try {
			fos = new FileOutputStream("/home/uranadh/Desktop/final.txt");
			fos.write(content);
			fos.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		System.out.println("Done");
	}
	
	/**
     * Run the thread.
     */
    @Override
    public void run() {
    	try {
        	nettyServer = this.createServer();
            nettyServer.startAndWait();
        } catch (Exception e) {
            log.error(e.getMessage() + "Error Happened when runing thread");
        }
    }
    
    public RxServer<ByteBuf, ByteBuf> getNettyServer(){
    	return nettyServer;
    }

}
