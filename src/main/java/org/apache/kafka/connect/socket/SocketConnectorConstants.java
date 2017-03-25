package org.apache.kafka.connect.socket;

public class SocketConnectorConstants {
	
	public enum MessageType{
		EVENT,METRIC,HEARTBEAT;
	}
	
	public static final String PARTITION_KEY = "partitionKey";
	//configuration files
	public static final String DOMAIN_CONFIG_FILE = "domain_config.json";
	public static final String QUERY_CONFIG_FILE = "query.json";
	public static final String APP_CONFIG_FILE = "app_config.properties";
	
	
	public static final String ALL_CONFIG_NAMES = "all_config_names";
	public static final String EVENTS_STATE_TRIGGER_ID = "events_stateTriggerId";
	public static final String EVENTS_OBJECTID = "events_objectId";
	public static final String METRICS_ID = "metrics_id";
	public static final String DOMAIN_NAMES = "domain_names";
	public static final String EVENTS_PARTITION_KEY = "events_partition_key";
	public static final String METRICS_PARTITION_KEY = "metrics_partition_key";
	public static final String DOMAIN_TOPIC_MAPPING = "domain_topic_mapping";
	
	public static final String DOMAIN_TAG_NAME = "domain";
	public static final String MSG_TYPE_TAG_NAME = "type";
	public static final String ERROR_TOPIC_CONFIG = "error_topic";
	
	public static final String CMDB_CONNECTION = "cmdb_connection";
	public static final String CURRENT_CACHE_DB = "current_cache_db";
	
	//queries
	public static class Query{
		public static final String EVENT_BY_STATETRIGGERID = "eventByStateTriggerId";
	}


}
