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

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.socket.Manager;
import org.apache.kafka.connect.socket.SocketConnectorConstants;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkProcessor {

	private static final Logger log = LoggerFactory.getLogger(BulkProcessor.class);
	private final ConcurrentLinkedQueue<RecordBatch> requests;
	private final int batchSize;
	private final long lingerMs;
	private static volatile boolean running;

	private static Converter converter;

	static {
		converter = new StringConverter();
		Map<String, String> configs = new HashMap<>();
		configs.put("schemas.enable", "false");
		converter.configure(configs, false);
	}

	public BulkProcessor(int maxInFlightRequests, int batchSize, long lingerMs, int maxRetry, long retryBackOffMs) {
		this.requests = new ConcurrentLinkedQueue<>();
		this.batchSize = batchSize;
		this.lingerMs = lingerMs;
	}

	public ConcurrentLinkedQueue<RecordBatch> getRequests() {
		return requests;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public long getLingerMs() {
		return lingerMs;
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


	public void addBytesToBatch(final byte[] request, final RecordBatch batch) {
		SourceRecord record = constructBulk(request);
		if (record != null) {
			batch.add(record);
		}
	}
	
	public void add(final RecordBatch batch){
		requests.add(batch);
	}


	private SourceRecord constructBulk(byte[] message) {
		// TODO identify topic and create key
		String topic = Manager.TOPICS.iterator().next();
		SourceRecord record = null;
		try {
			if (message.length < 20) {
				String errorMsg = " ***********************************  empty or incomplete message ************************  "
						+ message.length;
				log.error(errorMsg);
				return null;
			}

			final Map<String, String> partition = Collections.singletonMap(SocketConnectorConstants.PARTITION_KEY,
					SocketConnectorConstants.PARTITION_KEY);
			Charset charset = Charset.forName("UTF-8");
			String strMsg = new String(message, charset);
			record = new SourceRecord(partition, null, topic,ConnectSchema.STRING_SCHEMA,topic,ConnectSchema.STRING_SCHEMA, strMsg);

		} catch (Throwable ex) {
			StringBuilder error = new StringBuilder();
			StackTraceElement[] stacks = ex.getStackTrace();
			for (StackTraceElement stack : stacks) {
				error.append(stack.toString()).append("\n");
			}
		}
		return record;
	}

}
