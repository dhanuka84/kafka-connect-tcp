package org.apache.kafka.connect.socket.database;

import java.util.Map;

public abstract class AbstractDBManager {
	
	public enum DBType{
		ELASTICSEARCH;
	}
	
	public static AbstractDBManager getDBManager(DBType type) {
		AbstractDBManager manager = null;
		switch (type) {
		case ELASTICSEARCH:
			manager = ElasticSearchDBManager.get();

		default:
			manager = ElasticSearchDBManager.get();
		}
		return manager;
	}
	
	
	public abstract void start(final Map<String, String> config);
	
	public abstract void stop();
		
	
}
