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
import java.util.List;
import java.util.Map;

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
    private int batchSize;
    private String topicNames;
    private String name;

	private RxServer<byte[], byte[]> nettyServer;
	private Map<String, String> configProperties;
	
	private static Manager manager;
	private static BulkProcessor processor;

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
        try{
        	//create configurations
            SocketConnectorConfig config = new SocketConnectorConfig(map);
            
            port = config.getString(SocketConnectorConfig.CONNECTION_PORT_CONFIG);
            if (port == null || port.isEmpty())
                throw new ConnectException("Missing " + SocketConnectorConfig.CONNECTION_PORT_CONFIG + " config");

            batchSize = config.getInt(SocketConnectorConfig.BATCH_SIZE_CONFIG);
            
            name = config.getString(SocketConnectorConfig.NAME_CONFIG);
            if (name == null || name.isEmpty())
                throw new ConnectException("Missing " + SocketConnectorConfig.NAME_CONFIG + " config");
                   
            long lingerMs = config.getLong(SocketConnectorConfig.LINGER_MS_CONFIG);
            
            //create bulk processor
            processor = new BulkProcessor(); 
            //initialize manager
            manager = Manager.getManager();
            Manager.dumpConfiguration(map);
            Manager.addAllConfigToConnectMap(map);
            //Re map configuration from local file-app-config.properties to kafka connect
            Manager.reMapDomainConfigurations(map);
            //Re map configuration on the fly
            Manager.reMapDomainConfigurations(config, map, Manager.DOMAIN_TOPIC_MAP, Manager.CONNECTOR_RELATED_JSON_CONFIG);
   
            //initialize tcp server helper
            nettyServer = Manager.startTCPServer(Integer.parseInt(port.trim()), processor);
        }catch(Exception ex){
        	log.error(" Error occurred while starting connector ");
        }
      
        
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
    	log.info("stoping socket connector!");
    	if(nettyServer != null){
    		try {
				nettyServer.shutdown();
				processor.stop();
			} catch (InterruptedException ex) {
				log.error("Error while stoping TCP server thread" +ex.getMessage());
			}
    	}else{
    		log.error(" Netty Server can't be null!");
    	}
    }

	@Override
	public ConfigDef config() {
		return SocketConnectorConfig.config;
	}
	
	
}
