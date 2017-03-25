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

package org.apache.kafka.connect.socket.batch;

import static org.apache.kafka.connect.socket.SocketConnectorConstants.*;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.socket.Manager;
import org.apache.kafka.connect.socket.cache.CMDBManager;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import io.netty.util.internal.StringUtil;

public class BulkProcessor {

	private static final Logger log = LoggerFactory.getLogger(BulkProcessor.class);
	private static final ConcurrentLinkedQueue<RecordBatch> requests = new ConcurrentLinkedQueue<>();
	private static volatile boolean running;


	public BulkProcessor() {
	}

	public ConcurrentLinkedQueue<RecordBatch> getRequests() {
		return requests;
	}

	public void start() {
		running = true;
	}
	
	public void stop() {
		running = false;
	}

	public List<SourceRecord> process() {
		List<SourceRecord> records = null;
		try {
			if (running) {
				RecordBatch batch = requests.poll();
				records = batch.requests();
			}
		} catch (Exception e) {
			log.info("Work thread is interrupted, shutting down.");
		}
		return records;
	}


	public void addBytesToBatch(final byte[] request, final RecordBatch batch,Map<String,
			Set<String>> idMap, Map<String,String> domainTopicMap) {
		SourceRecord record = constructBulk(request, idMap, domainTopicMap);
		if (record != null) {
			batch.add(record);
		}
	}
	
	public void add(final RecordBatch batch){
		requests.add(batch);
	}

	//testing purpose
	public SourceRecord constructBulk(byte[] message, Map<String,Set<String>> idMap, Map<String,String> domainTopicMap) {
		SourceRecord record = null;
		String errorTopic = idMap.get(ERROR_TOPIC_CONFIG).iterator().next();
		try {
			if (message.length < 20) {
				String errorMsg = " **************** empty or incomplete message ************************  "+ message.length;
				log.error(errorMsg);
				return null;
			}

			
			Charset charset = Charset.forName("UTF-8");
			String jsonPayload = new String(message, charset);
			
			//md5 hash
			final JsonObject jsonObj = Manager.getJsonObject(jsonPayload);
			/*String eventsStateTriggerId = Manager.generateKeysUsingFields(jsonObj, EVENTS_STATE_TRIGGER_ID,idMap);
			String objectId = Manager.generateKeysUsingFields(jsonObj, EVENTS_OBJECTID,idMap);
			String md5StateTrigger = Manager.getMD5HexValue(eventsStateTriggerId);
			String md5ObjectId = Manager.getMD5HexValue(objectId);
			String objectIdFieldName = EVENTS_OBJECTID.substring(EVENTS_OBJECTID.indexOf("_") + 1);
			String stateTrigerIdFieldName = EVENTS_STATE_TRIGGER_ID.substring(EVENTS_STATE_TRIGGER_ID.indexOf("_") + 1);
			
			//add additional fields to payload
			Manager.addFieldToPayload(jsonObj, Collections.singletonMap(objectIdFieldName, md5ObjectId));
			Manager.addFieldToPayload(jsonObj, Collections.singletonMap(stateTrigerIdFieldName, md5StateTrigger));
			
			//create partition key
			String eventPartitionKey = Manager.generateKeysUsingFields(jsonObj, EVENTS_PARTITION_KEY,idMap);
			String eventPartitionKeyMD5 = Manager.getMD5HexValue(eventPartitionKey);*/
			
			
			String domain = jsonObj.get(DOMAIN_TAG_NAME).getAsString();
			String msgType = jsonObj.get(MSG_TYPE_TAG_NAME).getAsString();
			
			
			if(MessageType.EVENT.toString().equalsIgnoreCase(msgType)){
				//TODO cmdb cache access
				
				CMDBManager manager = CMDBManager.getCMDBManager(CMDBManager.CMDBType.REDIS);
				String value = manager.getValueBykey("");
			}
			
			String topic = null;
			if(StringUtil.isNullOrEmpty(domain) || StringUtil.isNullOrEmpty(msgType)){
				topic = errorTopic;
				log.error("Domain or msgType Tag is missing");
			}else{
				//TODO need to find msg type
				topic = domainTopicMap.get(msgType.toLowerCase()+"_"+domain.toLowerCase());
			}
			
			double random = Math.random();
			Map<String, String> partition = Collections.singletonMap(PARTITION_KEY,random+topic);
			record = new SourceRecord(partition, null, topic,ConnectSchema.STRING_SCHEMA,random+topic,
					ConnectSchema.STRING_SCHEMA, jsonObj.toString());

		} catch (Throwable ex) {
			log.error(Manager.getStackTrace(ex));
		}
		return record;
	}
	

}
