package org.apache.kafka.connect.socket.database;

import java.util.Map;
import org.apache.kafka.connect.socket.SocketConnectorConfig;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

public class ElasticSearchDBManager extends AbstractDBManager{
	
	private static final ElasticSearchDBManager manager = new ElasticSearchDBManager();
	private static final JestClientFactory factory = new JestClientFactory();
	private JestClient client;

	@Override
	public void start(Map<String, String> config) {
		String address = config.get(SocketConnectorConfig.CONNECTION_URL_CONFIG);
		factory.setHttpClientConfig(new HttpClientConfig.Builder(address).multiThreaded(true).build());
		this.client = factory.getObject();
		
	}

	@Override
	public void stop() {	
		client.shutdownClient();
	}
	
	public static AbstractDBManager get(){
		return manager;
	}

	public JestClient getClient() {
		return client;
	}

}
