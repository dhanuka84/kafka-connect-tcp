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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.socket.batch.BulkProcessor;
import org.apache.kafka.connect.socket.batch.RecordBatch;
import org.apache.kafka.connect.socket.cache.CMDBManager;
import org.apache.kafka.connect.socket.database.AbstractDBManager;
import org.apache.kafka.connect.socket.database.AbstractDBManager.DBType;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SocketSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(SocketSourceTask.class);
    
    /** All the ID/Key related mappings and domain names***/
    private final Map<String,Set<String>> ID_MAP = new HashMap<>(); 
    /** Domain and Topic mappings- domain name will be the key while topic will be the value**/
    private final Map<String,String> DOMAIN_TOPIC_MAP = new HashMap<>();
    private  BulkProcessor processor = null;
    private SocketConnectorConfig config = null;

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
		// create configurations
		log.info(" Socket task starting ");
		config = new SocketConnectorConfig(map);
		Manager.dumpConfiguration(map);
		Manager.reMapDomainConfigurations(config, map, DOMAIN_TOPIC_MAP, ID_MAP);
		processor = new BulkProcessor();
		//CMDBManager.getCMDBManager(CMDBManager.CMDBType.REDIS).start(map);
		DBType[] dbTypes = DBType.values();
		for(DBType type : dbTypes){
			AbstractDBManager.getDBManager(type).start(map);
		}
		log.info(" Socket task started ");

	}

    /**
     * Poll this SocketSourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
    	
		List<SourceRecord> records = null;
		long lingerMs = config.getLong(SocketConnectorConfig.LINGER_MS_CONFIG);
		int batchSize = config.getInt(SocketConnectorConfig.BATCH_SIZE_CONFIG);
		try {
			long now = System.currentTimeMillis();
			RecordBatch batch = new RecordBatch(now);
			int count = 0;
			while (!Manager.MESSAGES.isEmpty() || canSubmit(batch, System.currentTimeMillis(),lingerMs)) {
				if (++count > batchSize || canSubmit(batch, System.currentTimeMillis(), lingerMs)) {
					break;
				}
				byte[] bytes = Manager.MESSAGES.poll();
				if(bytes != null){
					//add bytes to batch
					processor.addBytesToBatch(bytes, batch, ID_MAP,DOMAIN_TOPIC_MAP);
					//add bytes to ConcurrentLinkedQueue
					processor.add(batch);
					records = processor.process();
				}				

			}
			
		} catch (Throwable ex) {
			String error = Manager.getStackTrace(ex);
			log.error("==============================="+ ((ex.getCause() != null ? ex.getCause().getMessage() + "\n" + error : "" + error)));
			log.error(" ========================= Payload : "+Manager.createPayLoad(records));
			return null;

		}

		if (records != null && !records.isEmpty()) {
			return records;
		}
        
                
        return null;
    }
    
    private boolean canSubmit(RecordBatch batch, long now, long lingerMs) {
    	return ( now - batch.getLastAttemptMs() > lingerMs);
    }

    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
    	/*CMDBManager manager = CMDBManager.getCMDBManager(CMDBManager.CMDBType.REDIS);
		manager.stop();*/
		DBType[] dbTypes = DBType.values();
		for(DBType type : dbTypes){
			AbstractDBManager.getDBManager(type).stop();
		}
    	log.info(" Socket task stopped ");
    }
    
}
