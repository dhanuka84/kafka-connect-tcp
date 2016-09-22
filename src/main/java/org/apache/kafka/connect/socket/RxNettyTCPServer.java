package org.apache.kafka.connect.socket;

import java.io.FileOutputStream;
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
	
	private static final Logger LOG = LoggerFactory.getLogger(RxNettyTCPServer.class);
	
	protected static final ConcurrentLinkedQueue<byte[]> messages = new ConcurrentLinkedQueue<>();
	private RxServer<ByteBuf, ByteBuf> nettyServer;

	static final int DEFAULT_PORT = 8791;

	private final int port;
	

	public RxNettyTCPServer(int port) {
		this.port = port;
	}

	public RxServer<ByteBuf, ByteBuf> createServer() {
		boolean debugEnabled = LOG.isDebugEnabled();
		
		RxServer<ByteBuf, ByteBuf> server = RxNetty
				.newTcpServerBuilder(port, new ConnectionHandler<ByteBuf, ByteBuf>() {
					@Override
					public Observable<Void> handle(final ObservableConnection<ByteBuf, ByteBuf> connection) {

						final AtomicInteger fullLenth = new AtomicInteger();
						final AtomicInteger count = new AtomicInteger();

						if(debugEnabled){
							LOG.debug(" ============================  New client connection established. " 
									+ Thread.currentThread().getId()+" ==============================");
						}
						
						if(connection.isCloseIssued()){
							LOG.error("============================   connection closed ============================  " );
						}
						
						Observable<ByteBuf> input = connection.getInput();
						Observable<Void> response = input.flatMap(new Func1<ByteBuf, Observable<Void>>() {

							@Override
							public Observable<Void> call(final ByteBuf originalBuff) {
								
								count.incrementAndGet();
								if(debugEnabled){
									LOG.debug(" ============================ " 
											+ " Max Capacity: " + originalBuff.maxCapacity()+" ==============================");
									LOG.debug(" ============================   Capacity: "+ originalBuff.capacity()+" ==============================");
								}
												
								byte[] bytes;
								int offset;
								int length = originalBuff.readableBytes();

								if (originalBuff.hasArray()) {
									bytes = originalBuff.array();
									offset = originalBuff.arrayOffset();
								} else {
									bytes = new byte[length];
									originalBuff.getBytes(originalBuff.readerIndex(), bytes);
									offset = 0;
								}
								
								fullLenth.addAndGet(length);
								
								if(debugEnabled){
									LOG.debug(" ============================ " + "onNext: " + " readable : " + originalBuff.isReadable()
											+ "  threadId" + Thread.currentThread().getId()+" ==============================");
									LOG.debug(" ============================   Full Length: "+ fullLenth+ " Lenght: "+ bytes.length+" ==============================");
						
								}
								
								if(fullLenth.get() == (Integer.MAX_VALUE - 5000)){
									LOG.info(" ============================ " + (Integer.MAX_VALUE - 5000) + " Number of messages reached to uper limit "+ "==============================");
									fullLenth.set(0);
								}
								
								Observable<Void> result = null;
								if (bytes.length > 0) {
									messages.add(bytes);
									connection.writeBytes(bytes);
									result = connection.writeBytesAndFlush("OK\r\n".getBytes());
								} else {
									if(debugEnabled){
										LOG.debug(" ============================ " + "Msg Empty: " + bytes.length+" ==============================");
									}
									result = Observable.empty();
								}
								
								if(debugEnabled){
									LOG.debug(" ============================ " + "Message Queue size : " + messages.size()+" ==============================");
								}
								
								return result;
								
								
							}
						}).subscribeOn(Schedulers.io())
						  .doOnCompleted(new Action0() {
							@Override
							public void call() {
								try {						
									if(debugEnabled){
										LOG.debug(" ============================ " + "Messages count : " + count+" ==============================");
									}
								} finally{
									count.set(0);
									fullLenth.set(0);
								}
							}
						});

						return response;
					}
				})
				
				.build();
		return server;
	}

	public static void main(final String[] args) {
		new RxNettyTCPServer(DEFAULT_PORT).createServer().startAndWait();
	}

	
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
            LOG.error(e.getMessage() + "Error Happened when runing TCP server thread");
        }
    }
    
    public RxServer<ByteBuf, ByteBuf> getNettyServer(){
    	return nettyServer;
    }

}
