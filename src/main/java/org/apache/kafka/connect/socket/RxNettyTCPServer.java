package org.apache.kafka.connect.socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
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

	private RxServer<ByteBuf, ByteBuf> nettyServer;

	static final int DEFAULT_PORT = SocketConnectorConfig.CONNECTION_PORT_DEFAULT;

	private final int port;
	

	public RxNettyTCPServer(int port) {
		this.port = port;
	}

	public RxServer<ByteBuf, ByteBuf> createServer() {
		//boolean debugEnabled = LOG.isDebugEnabled();
		boolean debugEnabled = true;
		
		RxServer<ByteBuf, ByteBuf> server = RxNetty
				.newTcpServerBuilder(port, new ConnectionHandler<ByteBuf, ByteBuf>() {
					@Override
					public Observable<Void> handle(final ObservableConnection<ByteBuf, ByteBuf> connection) {

						final AtomicInteger fullLenth = new AtomicInteger();
						final AtomicInteger count = new AtomicInteger();
						
						ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );

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
								
								ByteBuf buff = originalBuff.duplicate();
								count.incrementAndGet();
								if(debugEnabled){
									LOG.debug(" ============================ " 
											+ " Max Capacity: " + buff.maxCapacity()+" ==============================");
									LOG.debug(" ============================   Capacity: "+ buff.capacity()+" ==============================");
								}
												
								byte[] bytes;
								int offset;
								int length = buff.readableBytes();

								if (buff.hasArray()) {
									bytes = buff.array();
									offset = buff.arrayOffset();
								} else {
									bytes = new byte[length];
									buff.getBytes(buff.readerIndex(), bytes);
									offset = 0;
								}
								
								fullLenth.addAndGet(length);
								
								if(debugEnabled){
									LOG.debug(" ============================ " + "onNext: " + " readable : " + buff.isReadable()
											+ "  threadId" + Thread.currentThread().getId()+" ==============================");
									LOG.debug(" ============================   Full Length: "+ fullLenth+ " Lenght: "+ bytes.length+" ==============================");
						
								}
								
								if(fullLenth.get() == (Integer.MAX_VALUE - 5000)){
									LOG.info(" ============================ " + (Integer.MAX_VALUE - 5000) + " Number of messages reached to uper limit "+ "==============================");
									fullLenth.set(0);
								}
								
								Observable<Void> result = null;
								if (bytes.length > 0) {
									//Manager.MESSAGES.add(bytes);
									try {
										outputStream.write(bytes);
									} catch (IOException e) {
										LOG.error(" Error while storing byte array ");
									}
									//connection.writeBytes(bytes);
									result = connection.writeBytesAndFlush("OK\r\n".getBytes());
									//result = Observable.empty();
								} else {
									if(debugEnabled){
										LOG.debug(" ============================ " + "Msg Empty: " + bytes.length+" ==============================");
									}
									result = Observable.empty();
								}
								
								if(debugEnabled){
									LOG.debug(" ============================ " + "Message Queue size : " + Manager.MESSAGES.size()+" ==============================");
								}
								
								return result;
								
								
							}
						}).subscribeOn(Schedulers.io())
						  .doOnCompleted(new Action0() {
							@Override
							public void call() {
								try {		
									System.out.println("inside complete");
									byte[] wholeMsg = outputStream.toByteArray();
									Manager.MESSAGES.add(wholeMsg);
									//System.out.println(new String(wholeMsg));
									if(debugEnabled){
										LOG.debug(" ============================ " + "Messages count : " + count+" ==============================");
									}
								} finally{
									//count.set(0);
									fullLenth.set(0);
									try {
										outputStream.close();
									} catch (IOException ex) {
										LOG.error(" Error while closing BOS "+ex);
									}
								}
							}
						});

						return response;
					}
				})
				
				.build();
		return server;
	}

	public static void main(final String[] args) throws InterruptedException {
		 //initialize tcp server helper
		RxNettyTCPServer serverHelper = new RxNettyTCPServer(DEFAULT_PORT);
        
        new Thread(serverHelper).start();
        try {
			Thread.sleep(5000);
		} catch (InterruptedException ex) {
			LOG.error("Error while starting TCP server thread" +ex.getMessage());
		}
        RxServer<ByteBuf, ByteBuf> nettyServer = serverHelper.getNettyServer();
		Thread.sleep(5000l);
		
		Converter converter = new JsonConverter();
		Map<String, String> configs = new HashMap<>();
		configs.put("schemas.enable", "false");
		converter.configure(configs, false);
		while(true){
			byte [] bytes = Manager.MESSAGES.poll();
			if(bytes != null){
				SchemaAndValue value = converter.toConnectData("test", bytes);
				System.out.println(value.value());
				/*byte[] converted = converter.fromConnectData("test", ConnectSchema.STRING_SCHEMA, bytes);
				System.out.println(new String(converted));*/
			}
		}
		
		
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
