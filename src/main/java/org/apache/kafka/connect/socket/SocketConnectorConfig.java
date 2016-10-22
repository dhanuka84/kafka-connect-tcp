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

package org.apache.kafka.connect.socket;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

public class SocketConnectorConfig extends AbstractConfig {

  private static final String TCPSERVER_GROUP = "RxNettyTCPServer";
  private static final String CONNECTOR_GROUP = "Connector";
  
  protected static final String COMMON_GROUP = "Common";

  public static final String NAME_CONFIG = "name";
  private static final String NAME_DOC = "Globally unique name to use for this connector.";
  private static final String NAME_DISPLAY = "Connector name";

  public static final String CONNECTOR_CLASS_CONFIG = "connector.class";
  private static final String CONNECTOR_CLASS_DOC =
                  "Name or alias of the class for this connector. Must be a subclass of org.apache.kafka.connect.connector.Connector. " +
                  "If the connector is org.apache.kafka.connect.file.FileStreamSinkConnector, you can either specify this full name, " +
                  " or use \"FileStreamSink\" or \"FileStreamSinkConnector\" to make the configuration a bit shorter";
  private static final String CONNECTOR_CLASS_DISPLAY = "Connector class";
  
  public static final String CONNECTION_URL_CONFIG = "connection.url";
  private static final String CONNECTION_URL_DOC = "The URL to connect to Elasticsearch.";
  private static final String CONNECTION_URL_DISPLAY = "Connection URL";

  
  public static final String ERROR_TOPIC_CONFIG = "error_topic";
  private static final String ERROR_TOPIC_DOC = " topics that we gonna use for error handling";
  public static final String ERROR_TOPIC_DEFAULT = "error_topic";
  private static final String ERROR_TOPIC_DISPLAY = "error_topic";
  
  public static final String CONNECTION_PORT_CONFIG = "tcp.port";
  private static final String CONNECTION_PORT_DOC = "TCP port that client should connect";
  public static final int CONNECTION_PORT_DEFAULT = 8791;
  private static final String CONNECTION_PORT_DISPLAY = "TCP port";

  public static final String TASKS_MAX_CONFIG = "tasks.max";
  private static final String TASKS_MAX_DOC = "Maximum number of tasks to use for this connector.";
  public static final int TASKS_MAX_DEFAULT = 1;
  private static final int TASKS_MIN_CONFIG = 1;
  private static final String TASK_MAX_DISPLAY = "Tasks max";
  
  public static final String TYPE_NAME_CONFIG = "type.name";
  private static final String TYPE_NAME_DOC = "The type to use for each index.";
  private static final String TYPE_NAME_DISPLAY = "Type Name";

  public static final String KEY_IGNORE_CONFIG = "key.ignore";
  private static final String KEY_IGNORE_DOC =
      "Whether to ignore the key during indexing. When this is set to true, only the value from the message will be written to Elasticsearch."
      + "Note that this is a global config that applies to all topics. If this is set to true, "
      + "Use ``topic.key.ignore`` to config for different topics. This value will be overridden by the per topic configuration.";
  private static final boolean KEY_IGNORE_DEFAULT = false;
  private static final String KEY_IGNORE_DISPLAY = "Ignore Key";


  public static final String TOPIC_KEY_IGNORE_CONFIG = "topic.key.ignore";
  private static final String TOPIC_KEY_IGNORE_DOC =
      "A list of topics to ignore key when indexing. In case that the key for a topic can be null, you should include the topic in this config "
      + "in order to generate a valid document id.";
  private static final String TOPIC_KEY_IGNORE_DEFAULT = "";
  private static final String TOPIC_KEY_IGNORE_DISPLAY = "Topics to Ignore Key";

  public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
  private static final String FLUSH_TIMEOUT_MS_DOC = "The timeout when flushing data to Elasticsearch.";
  private static final long FLUSH_TIMEOUT_MS_DEFAULT = 10000;
  private static final String FLUSH_TIMEOUT_MS_DISPLAY = "Flush Timeout (ms)";

  public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
  private static final String MAX_BUFFERED_RECORDS_DOC =
      "Approximately the max number of records each task will buffer. This config controls the memory usage for each task. When the number of "
      + "buffered records is larger than this value, the partitions assigned to this task will be paused.";
  private static final long MAX_BUFFERED_RECORDS_DEFAULT = 100000;
  private static final String MAX_BUFFERED_RECORDS_DISPLAY = "Max Number of Records to Buffer";

  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC = "The number of requests to process as a batch when writing to Elasticsearch.";
  public static final int BATCH_SIZE_DEFAULT = 10000;
  private static final String BATCH_SIZE_DISPLAY = "Batch Size";

  public static final String LINGER_MS_CONFIG = "linger.ms";
  private static final String LINGER_MS_DOC =
      "The task groups together any records that arrive in between request transmissions into a single batched request. "
      + "Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the "
      + "tasks may want to reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount "
      + "of artificial delay. Rather than immediately sending out a record the task will wait for up to the given delay to allow other "
      + "records to be sent so that the sends can be batched together.";
  public static final long LINGER_MS_DEFAULT = 1;
  private static final String LINGER_MS_DISPLAY = "Linger (ms)";

  public static final String MAX_IN_FLIGHT_REQUESTS_CONFIG = "max.in.flight.requests";
  private static final String MAX_IN_FLIGHT_REQUESTS_DOC =
      "The maximum number of incomplete batches each task will send before blocking. Note that if this is set to be greater "
      + "than 1 and there are failed sends, there is a risk of message re-ordering due to retries";
  public static final int MAX_IN_FLIGHT_REQUESTS_DEFAULT = 5;
  private static final String MAX_IN_FLIGHT_REQUESTS_DISPLAY = "Max in Flight Requests";

  public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  private static final String RETRY_BACKOFF_MS_DOC =
      "The amount of time to wait before attempting to retry a failed batch. "
      + "This avoids repeatedly sending requests in a tight loop under some failure scenarios.";
  public static final long RETRY_BACKOFF_MS_DEFAULT = 100L;
  private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (ms)";

  public static final String MAX_RETRY_CONFIG = "max.retry";
  private static final String MAX_RETRY_DOC = "The max allowed number of retries. Allowing retries will potentially change the ordering of records.";
  public static final int MAX_RETRY_DEFAULT = 5;
  private static final String MAX_RETRY_DISPLAY = "Max Retry";

  public static final String TOPIC_SCHEMA_IGNORE_CONFIG = "topic.schema.ignore";
  private static final String TOPIC_SCHEMA_IGNORE_DOC = "A list of topics to ignore schema.";
  private static final String TOPIC_SCHEMA_IGNORE_DEFAULT = "";
  private static final String TOPIC_SCHEMA_IGNORE_DISPLAY = "Topics to Ignore Schema";
  
  public static final String EVENTS_STATETRIGGER_ID_CONFIG = "events_stateTriggerId";
  private static final String EVENTS_STATETRIGGER_ID_DOC = " events state trigger Id";
  private static final String EVENTS_STATETRIGGER_ID_DISPLAY = "events state trigger Id";
  
  public static final String EVENTS_OBJECT_ID_CONFIG = "events_objectId";
  private static final String EVENTS_OBJECT_ID_DOC = " events object Id";
  private static final String EVENTS_OBJECT_ID_DISPLAY = "events object Id";
  
  public static final String METRICS_ID_CONFIG = "metrics_id";
  private static final String METRICS_ID_DOC = " metrics Id";
  private static final String METRICS_ID_DISPLAY = "metrics id";
  
  public static final String METRICS_DOMAINS_CONFIG = "metrics_domains";
  private static final String METRICS_DOMAINS_DOC = " metrics domains";
  private static final String METRICS_DOMAINS_DISPLAY = "metrics domains";
  
  public static final String EVENTS_DOMAINS_CONFIG = "events_domains";
  private static final String EVENTS_DOMAINS_DOC = " events domains";
  private static final String EVENTS_DOMAINS_DISPLAY = "events domains";
  
  public static final String METRICS_PARTITION_KEY_CONFIG = "metrics_partition_key";
  private static final String METRICS_PARTITION_KEY_DOC = " metrics partition key";
  private static final String METRICS_PARTITION_KEY_DISPLAY = "metrics partition key";
  
  public static final String EVENTS_PARTITION_KEY_CONFIG = "events_partition_key";
  private static final String EVENTS_PARTITION_KEY_DOC = " events partition key";
  private static final String EVENTS_PARTITION_KEY_DISPLAY = "events partition key";
  
  public static final String DOMAIN_TOPIC_MAPPING_CONFIG = "domain_topic_mapping";
  private static final String DOMAIN_TOPIC_MAPPING_DOC = " domain topic mapping";
  private static final String DOMAIN_TOPIC_MAPPING_DISPLAY = "domain topic mapping";
  
  

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
    	.define(CONNECTION_URL_CONFIG, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC, TCPSERVER_GROUP, 1, Width.LONG,CONNECTION_URL_DISPLAY)
    	.define(NAME_CONFIG, Type.STRING, Importance.HIGH, NAME_DOC, COMMON_GROUP, 1, Width.MEDIUM, NAME_DISPLAY)
        .define(CONNECTOR_CLASS_CONFIG, Type.STRING, Importance.HIGH, CONNECTOR_CLASS_DOC, COMMON_GROUP, 2, Width.LONG, CONNECTOR_CLASS_DISPLAY)
    	.define(CONNECTION_PORT_CONFIG, Type.STRING, Importance.HIGH, CONNECTION_PORT_DOC, TCPSERVER_GROUP, 1, Width.LONG, CONNECTION_PORT_DISPLAY)	
        .define(TYPE_NAME_CONFIG, Type.STRING, Importance.HIGH, TYPE_NAME_DOC, TCPSERVER_GROUP, 1, Width.SHORT, TYPE_NAME_DISPLAY)
        .define(KEY_IGNORE_CONFIG, Type.BOOLEAN, KEY_IGNORE_DEFAULT, Importance.HIGH, KEY_IGNORE_DOC, CONNECTOR_GROUP, 2, Width.SHORT, KEY_IGNORE_DISPLAY)
        .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC, CONNECTOR_GROUP, 3, Width.SHORT, BATCH_SIZE_DISPLAY)
        .define(TASKS_MAX_CONFIG, Type.INT, TASKS_MAX_DEFAULT, Importance.MEDIUM, TASKS_MAX_DOC, CONNECTOR_GROUP, 4, Width.SHORT, TASK_MAX_DISPLAY)
        .define(MAX_IN_FLIGHT_REQUESTS_CONFIG, Type.INT, MAX_IN_FLIGHT_REQUESTS_DEFAULT, Importance.MEDIUM,MAX_IN_FLIGHT_REQUESTS_DOC, CONNECTOR_GROUP, 5, Width.SHORT,
                MAX_IN_FLIGHT_REQUESTS_DISPLAY)
        .define(TOPIC_KEY_IGNORE_CONFIG, Type.LIST, TOPIC_KEY_IGNORE_DEFAULT, Importance.LOW, TOPIC_KEY_IGNORE_DOC, CONNECTOR_GROUP, 7, Width.LONG, TOPIC_KEY_IGNORE_DISPLAY)
        
        .define(TOPIC_SCHEMA_IGNORE_CONFIG, Type.LIST, TOPIC_SCHEMA_IGNORE_DEFAULT, Importance.LOW, TOPIC_SCHEMA_IGNORE_DOC, CONNECTOR_GROUP, 9, Width.LONG, TOPIC_SCHEMA_IGNORE_DISPLAY)
        .define(LINGER_MS_CONFIG, Type.LONG, LINGER_MS_DEFAULT, Importance.LOW, LINGER_MS_DOC, CONNECTOR_GROUP, 10, Width.SHORT, LINGER_MS_DISPLAY)
        .define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, RETRY_BACKOFF_MS_DEFAULT, Importance.LOW, RETRY_BACKOFF_MS_DOC, CONNECTOR_GROUP, 11, Width.SHORT, RETRY_BACKOFF_MS_DISPLAY)
        .define(MAX_RETRY_CONFIG, Type.INT, MAX_RETRY_DEFAULT, Importance.LOW, MAX_RETRY_DOC, CONNECTOR_GROUP, 12, Width.SHORT, MAX_RETRY_DISPLAY)
        .define(FLUSH_TIMEOUT_MS_CONFIG, Type.LONG, FLUSH_TIMEOUT_MS_DEFAULT, Importance.LOW, FLUSH_TIMEOUT_MS_DOC, CONNECTOR_GROUP, 13, Width.SHORT, FLUSH_TIMEOUT_MS_DISPLAY)
        .define(MAX_BUFFERED_RECORDS_CONFIG, Type.LONG, MAX_BUFFERED_RECORDS_DEFAULT, Importance.LOW, MAX_BUFFERED_RECORDS_DOC, CONNECTOR_GROUP, 14, Width.SHORT, MAX_BUFFERED_RECORDS_DISPLAY)
    
    	.define(EVENTS_STATETRIGGER_ID_CONFIG, Type.LIST, "", Importance.HIGH, EVENTS_STATETRIGGER_ID_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, EVENTS_STATETRIGGER_ID_DISPLAY)
    	.define(EVENTS_OBJECT_ID_CONFIG, Type.LIST, "", Importance.HIGH, EVENTS_OBJECT_ID_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, EVENTS_OBJECT_ID_DISPLAY)
    	.define(METRICS_ID_CONFIG, Type.LIST, "", Importance.HIGH, METRICS_ID_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, METRICS_ID_DISPLAY)
    	.define(METRICS_DOMAINS_CONFIG, Type.LIST, "", Importance.HIGH, METRICS_DOMAINS_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, METRICS_DOMAINS_DISPLAY)
    	.define(EVENTS_DOMAINS_CONFIG, Type.LIST, "", Importance.HIGH, EVENTS_DOMAINS_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, EVENTS_DOMAINS_DISPLAY)
    	.define(METRICS_PARTITION_KEY_CONFIG, Type.LIST, "", Importance.HIGH, METRICS_PARTITION_KEY_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, METRICS_PARTITION_KEY_DISPLAY)
    	.define(EVENTS_PARTITION_KEY_CONFIG, Type.LIST, "", Importance.HIGH, EVENTS_PARTITION_KEY_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, EVENTS_PARTITION_KEY_DISPLAY)
    	.define(DOMAIN_TOPIC_MAPPING_CONFIG, Type.LIST, "", Importance.HIGH, DOMAIN_TOPIC_MAPPING_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, DOMAIN_TOPIC_MAPPING_DISPLAY)
    	
    	.define(ERROR_TOPIC_CONFIG, Type.LIST, ERROR_TOPIC_DEFAULT, Importance.LOW, ERROR_TOPIC_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, ERROR_TOPIC_DISPLAY)
    	
    	
    	;
  }

  static ConfigDef config = baseConfigDef();

  public SocketConnectorConfig(Map<String, String> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }
}
