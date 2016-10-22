package org.apache.kafka.connect.socket;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

public class JSONConfigTest {
	
	public static void main(String ...arg) throws IOException{
		/*FileHandler fileHandler = FileHandler.getInstance();
		JsonElement sample = FileHandler.getJSONElement("samplejson");
		JsonObject sampleObj = sample.getAsJsonObject();
		Gson gson = new Gson();
		
		Map<String,Set<String>> idMap = new HashMap<>(); 
		InputStream in = FileHandler.getResourceAsStream(SocketConnectorConstants.APP_CONFIG_FILE);
		Properties prop = new Properties();
		prop.load(in);
        JsonElement element = FileHandler.getJSONElement("sample-connect-config.json");
        if (element.isJsonObject()) {
            JsonObject jsonConfig = element.getAsJsonObject();
            jsonConfig.addProperty("test", "dhanuka");
            jsonConfig.addProperty("test", "ranasinghe");
            String msgIdString = prop.getProperty(SocketConnectorConstants.ALL_CONFIG_NAMES);
            String[] msgIds = msgIdString.split(",");
            for(String msgId : msgIds){
            	//System.out.println(msgId);
            	Set<String> keyValues = new HashSet<>();
            	String jsonArry = jsonConfig.get(msgId).getAsString();
            	 for (int i = 0; i < jsonArry.size(); i++) {
                     //System.out.println(sampleObj.get(jsonArry.get(i).getAsString()).getAsString());
            		// System.out.println(jsonArry.isJsonArray());
            		 if(msgId.equals("domain_topic_mapping")){
            			 Type stringStringMap = new TypeToken<Map<String, String>>(){}.getType();
            			 Map<String,String> map = gson.fromJson(jsonArry.toString().replace("[", "").replace("]", ""), stringStringMap);
            			 System.out.println(map);
            			 
            		 }else{
            			 keyValues.add(jsonArry.get(i).getAsString()); 
            		 }
                     
                 }
            	keyValues.addAll(Arrays.asList(jsonArry.split(",")));
            	idMap.put(msgId, keyValues);
            	
				if (msgId.equals(SocketConnectorConstants.DOMAIN_TOPIC_MAPPING)) {
					Type stringStringMap = new TypeToken<Map<String, String>>() {}.getType();
					Map<String, String> map = gson.fromJson("{"+jsonArry+"}", stringStringMap);
					//System.out.println(map);

				} 
            	 
                 //System.out.println(msgId+":"+jsonArry);//read as string
            }
            
            //System.out.println(idMap);
            Type stringStringMap = new TypeToken<Map<String, String>>() {}.getType();
            String json = gson.toJson(idMap,stringStringMap);

            System.out.println(json);
            ObjectMapper mapper = new ObjectMapper();
            String mapAsJson = mapper.writeValueAsString(idMap);
            System.out.println(mapAsJson);
            
            
            
            

            
            
            
        }
        */
		Map<String,String> map = new HashMap<>();
		Manager.getManager();
		 Manager.dumpConfiguration(map);
	        Manager.reMapDomainConfigurations(map);
	}

}
