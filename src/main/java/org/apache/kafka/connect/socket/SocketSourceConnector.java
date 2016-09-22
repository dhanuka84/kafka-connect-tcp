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
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.server.RxServer;

public class SocketSourceConnector extends SourceConnector {
    private final static Logger log = LoggerFactory.getLogger(SocketSourceConnector.class);

    public static final String PORT = "port";
    public static final String SCHEMA_NAME = "schema.name";
    public static final String BATCH_SIZE = "batch.size";
    public static final String TOPIC = "topic";
    public static final String SCHEMA_IGNORE = "schema.ignore";
    public static final String MAX_TASKS =  "tasks.max";
    public static final String NAME = "name";

    private String port;
    private String schemaName;
    private String batchSize;
    private String topic;
    private String name;
    private String maxTasks;
    private String schemaIgnore;
    
    private RxNettyTCPServer serverHelper;
	private RxServer<ByteBuf, ByteBuf> nettyServer;
	private Map<String, String> configProperties;

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

        port = map.get(PORT);
        if (port == null || port.isEmpty())
            throw new ConnectException("Missing " + PORT + " config");

        /*schemaName = map.get(SCHEMA_NAME);
        if (schemaName == null || schemaName.isEmpty())
            throw new ConnectException("Missing " + SCHEMA_NAME + " config");*/

        batchSize = map.get(BATCH_SIZE);
        if (batchSize == null || batchSize.isEmpty())
            throw new ConnectException("Missing " + BATCH_SIZE + " config");

        topic = map.get(TOPIC);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("Missing " + TOPIC + " config");
        
        name = map.get(NAME);
        if (name == null || name.isEmpty())
            throw new ConnectException("Missing " + NAME + " config");
        
        maxTasks = map.get(MAX_TASKS);
        if (maxTasks == null || maxTasks.isEmpty())
            throw new ConnectException("Missing " + MAX_TASKS + " config");
        
        schemaIgnore = map.get(SCHEMA_IGNORE);
        if (schemaIgnore == null || schemaIgnore.isEmpty())
            throw new ConnectException("Missing " + SCHEMA_IGNORE + " config");
        
        SocketConnectorConfig config = new SocketConnectorConfig(map);
        
        serverHelper = new RxNettyTCPServer(Integer.parseInt(port.trim()));
        
        new Thread(serverHelper).start();
        try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log.error(e.getMessage());
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
