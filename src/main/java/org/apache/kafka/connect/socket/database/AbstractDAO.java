package org.apache.kafka.connect.socket.database;

import com.google.gson.JsonObject;

public abstract class AbstractDAO {
	
	public abstract AbstractDBManager getDBManager();
	
	public abstract JsonObject getEventByStateTriggerId(String index, String type, String query) throws Exception;

}
