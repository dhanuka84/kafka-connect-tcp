/*******************************************************************************
 * Copyright [2016] [Dhanuka Ranasinghe]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.apache.kafka.connect.socket;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SocketSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(SocketSourceTask.class);

    private Integer port;
    private Integer batchSize = 100;
    private String topic;
    private static Schema schema = null;
    
    private static Converter converter;

    static {
      // Config the JsonConverter
      converter = new StringConverter();
      Map<String, String> configs = new HashMap<>();
      configs.put("schemas.enable", "false");
      converter.configure(configs, false);
    }

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

        topic = map.get(SocketSourceConnector.TOPIC);
    }

    /**
     * Poll this SocketSourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
    	
    	Thread.sleep(1000);
        List<SourceRecord> records = new ArrayList<>(0);
        byte[] message = null;
        try{
        	// while there are new messages in the socket queue
            System.out.println(" Size of the messages "+ RxNettyTCPServer.messages.size() + " List size "+ records.size());
            while (!RxNettyTCPServer.messages.isEmpty()) {
                // get the message
                message = RxNettyTCPServer.messages.poll();
                if(message.length < 5){
                	System.out.println(" ***********************************  empty message ************************  "+message.length);
                	continue;
                }
               
                final Map<String, String> partition = Collections.singletonMap(SocketConnectorConstants.PARTITION_KEY, topic);
                //SourceRecord record = new SourceRecord(partition, null, topic, ConnectSchema.BYTES_SCHEMA, message);
                Charset charset = Charset.forName("UTF-8");
                String strMsg = new String(message, charset);
                log.info(strMsg);
                byte[] fromConn = converter.fromConnectData(topic, ConnectSchema.STRING_SCHEMA,strMsg);
                strMsg = new String(fromConn, charset);
                log.info(strMsg);
                SourceRecord record = new SourceRecord(partition, null, topic, ConnectSchema.STRING_SCHEMA,strMsg);
                records.add(record);
                log.info(" Added Record "+message.length);
            }
        }catch(Throwable ex){
        	
        	StringBuilder error = new StringBuilder();
        	StackTraceElement[] stacks = ex.getStackTrace();
        	for(StackTraceElement stack : stacks){
        		error.append(stack.toString()).append("\n");
        	}
        	log.error("==============================="+((ex.getCause() != null? ex.getCause().getMessage() + " ++++++++ " + error.toString(): "" +error.toString())));
        	log.error(" ==============================  payload : "+ new String(message));
        	return null;
        	 
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
    
}
