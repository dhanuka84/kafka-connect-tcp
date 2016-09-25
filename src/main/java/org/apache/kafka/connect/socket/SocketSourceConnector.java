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
package org.apache.kafka.connect.socket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.socket.batch.BulkProcessor;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.server.RxServer;

public class SocketSourceConnector extends SourceConnector {
    private final static Logger log = LoggerFactory.getLogger(SocketSourceConnector.class);

    public static final String SCHEMA_NAME = "schema.name";


    private String port;
    private String batchSize;
    private String topic;
    private String name;
    private String maxTasks;
    private String schemaIgnore;
    
    private RxNettyTCPServer serverHelper;
	private RxServer<ByteBuf, ByteBuf> nettyServer;
	private Map<String, String> configProperties;
	
	public static Manager manager;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param map configuration settings
     */
    @Override
    public void start(Map<String, String> map) {
        log.info("Parsing configuration");
        
        configProperties = map;

        port = map.get(SocketConnectorConfig.CONNECTION_PORT_CONFIG);
        if (port == null || port.isEmpty())
            throw new ConnectException("Missing " + SocketConnectorConfig.CONNECTION_PORT_CONFIG + " config");

        batchSize = map.get(SocketConnectorConfig.BATCH_SIZE_CONFIG);
        int batchSizeValue = SocketConnectorConfig.BATCH_SIZE_DEFAULT;
        if (batchSize == null || batchSize.isEmpty())
            throw new ConnectException("Missing " + SocketConnectorConfig.BATCH_SIZE_CONFIG + " config");

        topic = map.get(SocketConnectorConfig.TOPICS_CONFIG);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("Missing " + SocketConnectorConfig.TOPICS_CONFIG + " config");
        
        String [] topics = topic.split(",");
        for(String name : topics){
        	Manager.TOPICS.add(name);
        }
        
        name = map.get(SocketConnectorConfig.NAME_CONFIG);
        if (name == null || name.isEmpty())
            throw new ConnectException("Missing " + SocketConnectorConfig.NAME_CONFIG + " config");
        
        maxTasks = map.get(SocketConnectorConfig.TASKS_MAX_CONFIG);
        if (maxTasks == null || maxTasks.isEmpty())
            throw new ConnectException("Missing " + SocketConnectorConfig.TASKS_MAX_CONFIG + " config");
        
        String maxInFlightRequests = map.get(SocketConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
        int maxInFlightRequestsValue = SocketConnectorConfig.MAX_IN_FLIGHT_REQUESTS_DEFAULT;
        if (!(maxInFlightRequests == null || maxInFlightRequests.isEmpty())){
        	maxInFlightRequestsValue = Integer.valueOf(maxInFlightRequests);
        }
               
        String lingerMs = map.get(SocketConnectorConfig.LINGER_MS_CONFIG);
        long lingerMsValue = SocketConnectorConfig.LINGER_MS_DEFAULT;
        if (!(lingerMs == null || lingerMs.isEmpty())){
        	lingerMsValue = Long.valueOf(lingerMs);
        }
        
        String maxRetry = map.get(SocketConnectorConfig.MAX_RETRY_CONFIG);
        int maxRetryValue = SocketConnectorConfig.MAX_RETRY_DEFAULT;
        if (!(maxRetry == null || maxRetry.isEmpty())){
        	maxRetryValue = Integer.valueOf(maxRetry);
        }
        
        String retryBackOffMs = map.get(SocketConnectorConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackOffMsValue = SocketConnectorConfig.RETRY_BACKOFF_MS_DEFAULT;
        if (!(retryBackOffMs == null || retryBackOffMs.isEmpty())){
        	retryBackOffMsValue = Long.valueOf(retryBackOffMs);
        }
        
        //create configurations
        SocketConnectorConfig config = new SocketConnectorConfig(map);
        //create bulk processor
        BulkProcessor processor = new BulkProcessor(maxInFlightRequestsValue, batchSizeValue, lingerMsValue
        		, maxRetryValue, retryBackOffMsValue); 
        //initialize manager
        manager = Manager.initialize(processor);
        //initialize tcp server helper
        serverHelper = new RxNettyTCPServer(Integer.parseInt(port.trim()));
        
        new Thread(serverHelper).start();
        processor.start();
        try {
			Thread.sleep(5000);
		} catch (InterruptedException ex) {
			log.error("Error while starting TCP server thread" +ex.getMessage());
		}
        nettyServer = serverHelper.getNettyServer();
        dumpConfiguration(map);
    }

    /**
     * Returns the Task implementation for this Connector.
     *
     * @return tha Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return SocketSourceTask.class;
    }

    /**
     * Returns a set of configurations for the Task based on the current configuration.
     * It always creates a single set of configurations.
     *
     * @param i maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
      List<Map<String, String>> taskConfigs = new ArrayList<>();
      Map<String, String> taskProps = new HashMap<>();
      taskProps.putAll(configProperties);
      for (int i = 0; i < maxTasks; i++) {
        taskConfigs.add(taskProps);
      }
      return taskConfigs;
    }
    /**
     * Stop this connector.
     */
    @Override
    public void stop() {
    	if(nettyServer != null){
    		try {
				nettyServer.shutdown();
				Manager.getProcessor().stop();
			} catch (InterruptedException ex) {
				log.error("Error while stoping TCP server thread" +ex.getMessage());
			}
    	}
    }

    private void dumpConfiguration(Map<String, String> map) {
        log.info("Starting connector with configuration:");
        for (Map.Entry entry : map.entrySet()) {
            log.info("{}: {}", entry.getKey(), entry.getValue());
        }
    }

	@Override
	public ConfigDef config() {
		return SocketConnectorConfig.config;
	}
	
	
}
