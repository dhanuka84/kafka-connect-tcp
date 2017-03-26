package org.apache.kafka.connect.socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.PooledByteBufAllocator;
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
import rx.functions.Action0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

public final class RxNettyTCPServer implements Runnable{
	
	private static final Logger LOG = LoggerFactory.getLogger(RxNettyTCPServer.class);

	private RxServer<byte[], byte[]> nettyServer;

	static final int DEFAULT_PORT = SocketConnectorConfig.CONNECTION_PORT_DEFAULT;

	private final int port;
	

	public RxNettyTCPServer(int port) {
		this.port = port;
	}
	
	/* private class MyStringServerFactory implements ChannelPipelineFactory{
		   public ChannelPipeline getPipeline() throws Exception {
		     ChannelPipeline p = Channels.pipeline();
		     // Decoders
		     p.addLast("frameDecoder", new DelimiterBasedFrameDecoder(Delimiters.lineDelimiter()));
		     p.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8));
		     // Encoder
		     p.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8));
		     return p; 
		   } 
		}*/

	public RxServer<byte[], byte[]> createServer() {
		//boolean debugEnabled = LOG.isDebugEnabled();
		boolean debugEnabled = true;

		PipelineConfigurator<byte[], byte[]>  config = PipelineConfigurators.byteArrayConfigurator();
		RxServer<byte[], byte[]> server = RxNetty
				.newTcpServerBuilder(port, new ConnectionHandler<byte[], byte[]>() {
					@Override
					public Observable<Void> handle(final ObservableConnection<byte[], byte[]> connection) {

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
						Observable<Void> response = input.flatMap(new Func1<byte[], Observable<Void>>() {

							@Override
							public Observable<Void> call(final byte[] originalBuff) {
			
								/*byte[] buff = originalBuff.duplicate();*/
								count.incrementAndGet();
								/*if(debugEnabled){
									LOG.info(" ============================ " 
											+ " Max Capacity: " + buff.maxCapacity()+" ==============================");
									LOG.info(" ============================   Capacity: "+ buff.capacity()+" ==============================");
								}*/
												
								/*byte[] bytes;
								int offset;
								int length = buff.readableBytes();

								if (buff.hasArray()) {
									bytes = buff.array();
									offset = buff.arrayOffset();
								} else {
									bytes = new byte[length];
									buff.getBytes(buff.readerIndex(), bytes);
									offset = 0;
								}*/
								
								byte[] bytes = originalBuff;
								if(bytes == null){
									bytes = new byte[0];
								}
								int length = bytes.length;
								
								fullLenth.addAndGet(length);
								
								/*if(debugEnabled){
									LOG.info(" ============================ " + "onNext: " + " readable : " + buff.isReadable()
											+ "  threadId" + Thread.currentThread().getId()+" ==============================");
									LOG.info(" ============================   Full Length: "+ fullLenth+ " Lenght: "+ bytes.length+" ==============================");
						
								}*/
								
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
						}).subscribeOn(Schedulers.io())
						  .doOnCompleted(new Action0() {
							@Override
							public void call() {
								try {		
									System.out.println("inside complete");
									byte[] wholeMsg = outputStream.toByteArray();
									Manager.MESSAGES.add(wholeMsg);
									//System.out.println(new String(wholeMsg));
									//System.out.println(new String(wholeMsg));
									//TODO write after complete
									Observable<Void> result = null;
									//connection.writeBytesAndFlush("OK\r\n".getBytes());
									/*if(!connection.isCloseIssued()){
										connection.close(true);
									}*/
									
									if(debugEnabled){
										LOG.info(" ============================ " + "Messages count : " + count+" ==============================");
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

	public static void main(final String[] args) throws InterruptedException {
		 //initialize tcp server helper        
		Manager.startTCPServer(DEFAULT_PORT, null);
		
		Converter converter = new JsonConverter();
		Map<String, String> configs = new HashMap<>();
		configs.put("schemas.enable", "false");
		converter.configure(configs, false);
		while(true){
			byte [] bytes = Manager.MESSAGES.poll();
			if(bytes != null){
				SchemaAndValue value = converter.toConnectData("test", bytes);
				System.out.println(value.value());
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
    
    public RxServer<byte[], byte[]> getNettyServer(){
    	return nettyServer;
    }

}
