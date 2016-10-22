package org.apache.kafka.connect.socket.cache;

import java.util.List;
import java.util.Map;

public abstract class CMDBManager {
	
	public enum CMDBType{
		REDIS;
	}
	
	public static CMDBManager getCMDBManager(CMDBType type) {
		CMDBManager manager = null;
		switch (type) {
		case REDIS:
			manager = RedisCMDBManager.get();

		default:
			manager = RedisCMDBManager.get();
		}
		return manager;
	}
	
	public abstract String getValueBykey(String key);
	
	public abstract void start(final Map<String, String> config);
	
	public abstract void stop();

}
