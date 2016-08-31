package org.apache.kafka.connect.socket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.server.RxServer;

/**
 * SocketSourceTask is a Task that reads records from a Socket for storage in Kafka.
 *
 * @author Andrea Patelli
 */
public class SocketSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(SocketSourceTask.class);

    private Integer port;
    private Integer batchSize = 100;
    private String schemaName;
    private String topic;
    private static Schema schema = null;
    
  

    @Override
    public String version() {
        return new SocketSourceConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param map initial configuration
     */
    @Override
    public void start(Map<String, String> map) {
        try {
            port = Integer.parseInt(map.get(SocketSourceConnector.PORT));
        } catch (Exception e) {
            throw new ConnectException(SocketSourceConnector.PORT + " config should be an Integer");
        }

        try {
            batchSize = Integer.parseInt(map.get(SocketSourceConnector.BATCH_SIZE));
        } catch (Exception e) {
            throw new ConnectException(SocketSourceConnector.BATCH_SIZE + " config should be an Integer");
        }

       /* schemaName = map.get(SocketSourceConnector.SCHEMA_NAME);*/
        topic = map.get(SocketSourceConnector.TOPIC);

        log.trace("Creating schema");
        /*schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("message", Schema.OPTIONAL_STRING_SCHEMA)
                .build();*/


        log.trace("Opening Socket");
       /* socketServerThread = new SocketServerThread(port);
        new Thread(socketServerThread).start();*/
        
        
       /* serverHelper = new RxNettyTCPServer(port, messages);
        nettyServer = serverHelper.createServer();
        nettyServer.startAndWait();*/
    }

    /**
     * Poll this SocketSourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    /*@Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>(0);
        // while there are new messages in the socket queue
        while (!socketServerThread.messages.isEmpty() && records.size() < batchSize) {
            // get the message
            String message = socketServerThread.messages.poll();
            // creates the structured message
            Struct messageStruct = new Struct(schema);
            messageStruct.put("message", message);
            // creates the record
            // no need to save offsets
            SourceRecord record = new SourceRecord(Collections.singletonMap("socket", 0), Collections.singletonMap("0", 0), topic, messageStruct.schema(), messageStruct);
            records.add(record);
        }
        return records;
    }*/
    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
    	
    	Thread.sleep(1000);
        List<SourceRecord> records = new ArrayList<>(0);
        // while there are new messages in the socket queue
        System.out.println(" Size of the messages "+ RxNettyTCPServer.messages.size() + " List size "+ records.size());
        while (!RxNettyTCPServer.messages.isEmpty() && records.size() < batchSize) {
            // get the message
            byte[] message = RxNettyTCPServer.messages.poll();
            // creates the structured message
           /* Struct messageStruct = new Struct(schema);
            messageStruct.put("message", message);*/
            // creates the record
            // no need to save offsets
            SourceRecord record = new SourceRecord(Collections.singletonMap("socket", 0), Collections.singletonMap("0", 0), topic, null, new String(message));
            records.add(record);
            log.info(" Added Record "+message.length);
        }
        if(!records.isEmpty()){
        	return records;
        }
        
                
        return null;
    }

    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
    	
        //socketServerThread.stop();
    }
    
	@Override
	public void initialize(SourceTaskContext context) {
		super.initialize(context);
	}
}
