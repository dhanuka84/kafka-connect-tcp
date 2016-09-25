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

import org.apache.kafka.connect.socket.batch.BulkProcessor;
import org.apache.kafka.connect.socket.batch.RecordBatch;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SocketSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(SocketSourceTask.class);
    
    private static Converter converter;

    static {
      // Config the String Converter
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
		BulkProcessor processor = Manager.getProcessor();
		int batchSize = processor.getBatchSize();
		try {
			long now = System.currentTimeMillis();
			RecordBatch batch = new RecordBatch(now);
			int count = 1;
			while (!Manager.MESSAGES.isEmpty() || canSubmit(batch, System.currentTimeMillis(), processor.getLingerMs())) {
				if (++count > batchSize || canSubmit(batch, System.currentTimeMillis(), processor.getLingerMs())) {
					break;
				}
				byte[] bytes = Manager.MESSAGES.poll();
				if(bytes != null){
					//add bytes to batch
					processor.addBytesToBatch(bytes, batch);
					//add bytes to queue
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
    	log.info(" Socket task stopped ");
    }
    
}
