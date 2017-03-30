package org.apache.kafka.connect.socket;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.channel.ConnectionHandler;
import io.reactivex.netty.channel.ObservableConnection;
import io.reactivex.netty.pipeline.PipelineConfigurator;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.server.RxServer;
import rx.Observable;
import rx.Observer;
import rx.functions.Func1;

public final class RxNettyTCPServer2 implements Runnable{
	
	private static final Logger LOG = LoggerFactory.getLogger(RxNettyTCPServer2.class);

	private RxServer<byte[], byte[]> nettyServer;

	static final int DEFAULT_PORT = SocketConnectorConfig.CONNECTION_PORT_DEFAULT;

	private final int port;
	

	public RxNettyTCPServer2(int port) {
		this.port = port;
		nettyServer = this.createServer();
	}
	
	private final FutureConnection futureConnection = new FutureConnection();
	private final FutureTask<ObservableConnection<byte[], byte[]>> future = new FutureTask<>(futureConnection);
	
	private static class FutureConnection implements Callable<ObservableConnection<byte[],byte[]>> {

		private ObservableConnection<byte[], byte[]> connection;
		
		public void setConnection(final ObservableConnection<byte[], byte[]> connection) {
			if (connection == null)
			      throw new IllegalArgumentException();
			if (this.connection != null)
				//throw new IllegalStateException("Immutable variable");
			this.connection = connection;
		}

		@Override public ObservableConnection<byte[], byte[]> call() throws Exception {
			return connection;
		}
	}
	
	
	public RxServer<byte[], byte[]> createServer() {
		//boolean debugEnabled = LOG.isDebugEnabled();
		boolean debugEnabled = true;

		PipelineConfigurator<byte[], byte[]>  config = PipelineConfigurators.byteArrayConfigurator();
		RxServer<byte[], byte[]> server = RxNetty
				.newTcpServerBuilder(port, new ConnectionHandler<byte[], byte[]>() {
					@Override
					public Observable<Void> handle(final ObservableConnection<byte[], byte[]> connection) {
						
						LOG.info("connect client {}", connection.getChannel()
								.remoteAddress());
						
						

					/*	futureConnection.setConnection(connection);
						future.run();*/
						
						//Observable.from(connection)

						return Observable.never();
/*
						final AtomicInteger fullLenth = new AtomicInteger();
						final AtomicInteger count = new AtomicInteger();
						
						ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );

						if(debugEnabled){
							LOG.info(" ============================  New client connection established. " 
									+ Thread.currentThread().getId()+" ==============================");
						}
						
						if(connection.isCloseIssued()){
							LOG.error("============================   connection closed ============================  " );
						}
						
						Observable<byte[]> input = connection.getInput();
						Observable<Void> response = 
						
								*/
							/*input.flatMap(new Func1<byte[], Observable<Void>>() {

							@Override
							public Observable<Void> call(final byte[] originalBuff) {
								count.incrementAndGet();							
								byte[] bytes = originalBuff;
								if(bytes == null){
									bytes = new byte[0];
								}
								int length = bytes.length;								
								fullLenth.addAndGet(length);
								
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
									connection.writeBytes(bytes);
									//result = connection.writeBytesAndFlush("OK\r\n".getBytes());
									result = Observable.empty();
								} else {
									if(debugEnabled){
										LOG.info(" ============================ " + "Msg Empty: " + bytes.length+" ==============================");
									}
									result = Observable.empty();
								}
								
								if(debugEnabled){
									LOG.info(" ============================ " + "Message Queue size : " + Manager.MESSAGES.size()+" ==============================");
								}
								
								return result;
								
								
							}
						})*//*.subscribeOn(Schedulers.io())
						  .doOnCompleted(new Action0() {
							@Override
							public void call() {
								try {		
									System.out.println("inside complete");
									byte[] wholeMsg = outputStream.toByteArray();
									//Manager.MESSAGES.add(wholeMsg);
									//System.out.println(new String(wholeMsg));
									//System.out.println(new String(wholeMsg));
									//TODO write after complete
									Observable<Void> result = null;
									//connection.writeBytesAndFlush("OK\r\n".getBytes());
									if(!connection.isCloseIssued()){
										connection.close(true);
									}
									String msg = new String(wholeMsg);
									if(debugEnabled){
										LOG.info(" ============================ " + "Messages count : " + msg+" ==============================");
									}
									System.out.println(msg);
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

						return response;*/
					}
				})
				.appendPipelineConfigurator(config)
				.channelOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(1*1024*1024))
				//.channelOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
				.channelOption(ChannelOption.SO_KEEPALIVE, true)
				.channelOption(ChannelOption.SO_SNDBUF, 1*1024*1024)
				.channelOption(ChannelOption.SO_RCVBUF, 1*1024*1024)
				.channelOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(32*1024*1024, 100*1024*1024))
				
				//.channelOption(ChannelOption.TCP_NODELAY,true)
				
				
				.build();
		return server;
	}
	
	private RxNettyTCPServer2 receiveMessages() {	
		Observable.from(future)
				.flatMap(new Func1<ObservableConnection<byte[], byte[]>, Observable<byte[]>>() {

					@Override public Observable<byte[]> call(
							ObservableConnection<byte[], byte[]> connection) {

						return connection.getInput();
					}
				})
				.subscribe(new Observer<byte[]>() {

					@Override public void onNext(byte[] s) {
						LOG.info("received : \"{}\"", new String(s));
//						sendMessage(s);
					}

					@Override public void onError(Throwable e) {
						LOG.error(e.getMessage(), e);
					}

					@Override public void onCompleted() {
						LOG.info("received onComplete");
						
					}
				});

		return this;
	}

	private RxNettyTCPServer2 sendMessage(byte[] sendMessage) {
		Observable.from(future)
				.flatMap(new Func1<ObservableConnection<byte[], byte[]>, Observable<Void>>() {

					@Override public Observable<Void> call(
							ObservableConnection<byte[], byte[]> connection) {
						return connection.writeStringAndFlush(new String(sendMessage) + "\n");
					}
				})
				.subscribe(new Observer<Void>() {

					@Override public void onNext(Void t) {
						LOG.info("onNext : " + t);
					}

					@Override public void onError(Throwable e) {
						LOG.error(e.getMessage(), e);
					}

					@Override public void onCompleted() {
						LOG.info("sent : \"{}\"", sendMessage);
					}
				});

		return this;
	}

	public static void main(final String[] args) throws InterruptedException {
		
		RxNettyTCPServer2 serverHelper = new RxNettyTCPServer2(DEFAULT_PORT);
        new Thread(serverHelper).start();
		 //initialize tcp server helper        
		//Manager.startTCPServer(DEFAULT_PORT, null);
		
		/*Converter converter = new JsonConverter();
		Map<String, String> configs = new HashMap<>();
		configs.put("schemas.enable", "false");
		converter.configure(configs, false);
		while(true){
			byte [] bytes = Manager.MESSAGES.poll();
			if(bytes != null){
				SchemaAndValue value = converter.toConnectData("test", bytes);
				System.out.println(value.value());
			}
		}*/
		
		
	}

	
	/**
     * Run the thread.
     */
    @Override
    public void run() {
    	try {
        	//nettyServer = this.createServer();
            //nettyServer.startAndWait();
        	this.startAndWait();
        } catch (Exception e) {
            LOG.error(e.getMessage() + "Error Happened when runing TCP server thread");
        }
    }
    
    private void startAndWait() {
    	nettyServer.start();
    	this.receiveMessages();
		this.sendMessage("1".getBytes());
        try {
        	nettyServer.waitTillShutdown();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }
    
    public RxServer<byte[], byte[]> getNettyServer(){
    	return nettyServer;
    }

}
