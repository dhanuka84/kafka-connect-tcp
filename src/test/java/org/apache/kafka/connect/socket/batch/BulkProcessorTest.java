/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package org.apache.kafka.connect.socket.batch;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.socket.SocketConnectorConstants;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.Before;
import org.junit.Test;

public class BulkProcessorTest {

  private volatile int numFailure = 0;
  private volatile int numSuccess = 0;
  private volatile int numExecute = 0;
  private final byte[] dummyPayload = ByteBuffer.allocate(0).array();
  private final String index = "test";
  private final String type = "connect";
  private final String topic = "topic";
  private final int partition = 0;
  private final long flushTimeoutMs = 30000;
  private final long lingerMs = 2000;
  private final int maxRetry = 5;
  private final long retryBackoffMs = 3000;
  
  private static Converter converter;

  static {
    // Config the JsonConverter
    converter = new StringConverter();
    Map<String, String> configs = new HashMap<>();
    configs.put("schemas.enable", "false");
    converter.configure(configs, false);
  }


  @Before
  public void setUp() {
    synchronized (this) {
      numExecute = 0;
      numSuccess = 0;
      numFailure = 0;
    }
  }
  
	private void addRecords(BulkProcessor bulkProcessor, int numRecords) {
		for (int offset = 0; offset < numRecords; ++offset) {
			String id = topic + "+" + partition + "+" + offset;
			final Map<String, String> partition = Collections.singletonMap(SocketConnectorConstants.PARTITION_KEY,
					topic);
			Charset charset = Charset.forName("UTF-8");
			String strMsg = new String(dummyPayload, charset);
			byte[] fromConn = converter.fromConnectData(topic, ConnectSchema.STRING_SCHEMA, strMsg);
			strMsg = new String(fromConn, charset);
			SourceRecord esRequest = new SourceRecord(partition, null, topic, ConnectSchema.STRING_SCHEMA, strMsg);
			//bulkProcessor.add(esRequest);
		}
	}

  @Test
  public void testWrite() throws Throwable {
    int maxInFlightRequests = 2;
    int batchSize = 5;
    int numRecords = 10;

    BulkProcessor bulkProcessor = new BulkProcessor();
    bulkProcessor.start();

    addRecords(bulkProcessor, numRecords);

   /* int numIncompletes = bulkProcessor.getNumIncompletes();
    assertEquals(0, numIncompletes);*/
    assertEquals(0, numFailure);
    assertEquals(2, numSuccess);
    assertEquals(2, numExecute);
  }


}
