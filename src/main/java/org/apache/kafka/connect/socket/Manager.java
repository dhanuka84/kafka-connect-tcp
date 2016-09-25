package org.apache.kafka.connect.socket;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.connect.socket.batch.BulkProcessor;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Manager {
	private final static Logger log = LoggerFactory.getLogger(Manager.class);
    
    public static final ConcurrentLinkedQueue<byte[]> MESSAGES = new ConcurrentLinkedQueue<>();
    public final static Set<String> TOPICS = new HashSet<String>();
    private final BulkProcessor processor;
    private static Manager manager;

	private Manager(final BulkProcessor processor) {
		super();
		this.processor = processor;
	}
	
	public static Manager initialize(final BulkProcessor processor){
		manager = new Manager(processor); 
		return manager;
	}
	
	public static BulkProcessor getProcessor(){
		return manager.processor;
	}
	
	public static String getStackTrace(Throwable th){
		StringBuilder error = new StringBuilder();
    	StackTraceElement[] stacks = th.getStackTrace();
    	for(StackTraceElement stack : stacks){
    		error.append(stack.toString()).append("\n");
    	}
    	return error.toString();
	}
	
	public static String createPayLoad(List<SourceRecord> records){
		StringBuilder payload = new StringBuilder();
		if(records == null || records.isEmpty()){
			return payload.toString();
		}
		
		for(SourceRecord record : records){
			payload.append(record).append("\n");
		}
		return payload.toString();
	}

}
