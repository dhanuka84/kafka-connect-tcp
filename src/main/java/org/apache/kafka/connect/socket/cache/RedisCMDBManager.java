package org.apache.kafka.connect.socket.cache;

import static org.apache.kafka.connect.socket.SocketConnectorConstants.*;

import java.util.Map;

import com.lambdaworks.redis.ReadFrom;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.codec.Utf8StringCodec;
import com.lambdaworks.redis.masterslave.MasterSlave;
import com.lambdaworks.redis.masterslave.StatefulRedisMasterSlaveConnection;

public class RedisCMDBManager extends CMDBManager {

	private RedisClient currentCacheClient;
	private RedisClient activeOneClient;
	private RedisClient activeTwoClient;
	private String connString;
	private RedisURI currentCacheURI;
	private RedisURI activeOneURI;
	private RedisURI activeTwoURI;
	private Map<String, String> config;
	private String currentCacheDb;
	private static final Utf8StringCodec codec = new Utf8StringCodec();

	private static final RedisCMDBManager manager = new RedisCMDBManager();

	private RedisCMDBManager() {

	}

	public void start(final Map<String, String> config) {
		// Syntax:
		// redis-sentinel://[password@]host[:port][,host2[:port2]][/databaseNumber]#sentinelMasterId
		// "redis-sentinel://localhost:26379,localhost:26380/0#gmemaster"
		//https://github.com/mp911de/lettuce/wiki/Redis-URI-and-connection-details
		this.config = config;
		this.connString = config.get(CMDB_CONNECTION);
		this.currentCacheDb = config.get(CURRENT_CACHE_DB);
		this.currentCacheURI = RedisURI.create(connString.replaceFirst("[0-9]+\\#", "0#"));
		this.activeOneURI = RedisURI.create(connString.replaceFirst("[0-9]+\\#", "1#"));
		this.activeTwoURI = RedisURI.create(connString.replaceFirst("[0-9]+\\#", "2#"));
		currentCacheURI.setDatabase(0);
		activeOneURI.setDatabase(1);
		activeTwoURI.setDatabase(2);
		
		if (currentCacheClient == null) {
			currentCacheClient = RedisClient.create();
		}
		
		if (activeOneClient == null) {
			activeOneClient = RedisClient.create();
		}
		
		if (activeTwoClient == null) {
			activeTwoClient = RedisClient.create();
		}
	}

	public static RedisCMDBManager get() {
		return manager;
	}
	

	@Override
	public String getValueBykey(final String key) {
		RedisCommands<String, String> sync = getConnection().sync();
		String value = sync.get(key); // master read

		if (sync.isOpen()) {
			sync.close();
		}
		return value;
	}
	
	private StatefulRedisConnection<String,String> getConnection(RedisClient client, RedisURI uri){
		StatefulRedisMasterSlaveConnection<String, String> connection = MasterSlave.connect(client, codec,uri);
		connection.setReadFrom(ReadFrom.MASTER_PREFERRED);
		return connection;
	}
	
	private StatefulRedisConnection<String,String> getConnection(){
		StatefulRedisConnection<String, String> connection = getConnection(currentCacheClient, currentCacheURI);
		RedisCommands<String, String> sync = connection.sync();
		String value = sync.get(currentCacheDb); // master read
		int intValue = Integer.parseInt(value);

		if (sync.isOpen()) {
			sync.close();
		}
		
		RedisClient client = null;
		RedisURI uri = null;
		
		if(intValue == 1){
			client = activeOneClient;
			uri = activeOneURI;
		}else{
			client = activeTwoClient;
			uri = activeTwoURI;
		}
		
		connection = getConnection(client, uri);		
		return connection;
	} 

	@Override
	public void stop() {
		currentCacheClient.shutdown();
		activeOneClient.shutdown();
		activeTwoClient.shutdown();

	}

}
