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

import static java.nio.file.Files.readAllBytes;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;


public class FileHandler {

	private final static Logger log = LoggerFactory.getLogger(FileHandler.class);
	private static final ThreadLocal<FileHandler> threadLocal = new ThreadLocal<FileHandler>() {
		protected FileHandler initialValue() {
			return new FileHandler();
		}
	};
	
	
	private FileHandler() {
		
    }
    
    public static FileHandler getInstance() {
        return threadLocal.get();
    }
	
	public static InputStream getResourceAsStream(final String fileName){
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
		return is;
	}
	
	public static String readInputStream(final InputStream is) throws Exception{
		
		if(is == null){
			return null;
		}
		
		String theString = null;
		StringWriter writer = new StringWriter();
		try{
			
			Charset charset = Charset.forName("UTF-8");
			IOUtils.copy(is, writer, charset);
			theString = writer.toString();
		}finally{
			try{
				close(writer);
				close(is);
			}catch(Exception ex){
				
			}
		}
		return theString;
		
	}
	
	public static JsonElement getJSONElement(final String fileName) {
		InputStream partitionKeyIS = FileHandler.getResourceAsStream(fileName);
		JsonParser parser = new JsonParser();
		JsonElement element = null;
		try {
			element = parser.parse(readInputStream(partitionKeyIS));
		} catch (Exception e) {
			log.error(" Error while reading " + fileName);
		}
		return element;
	}
	
	public static void deleteAndWriteToFile(final String fileLocation, final String output) throws IOException {

		BufferedWriter bw = null;
		FileWriter fw = null;
		try{
			File file = new File(fileLocation);
			
			//System.out.println("output : "+output);

			// if file doesnt exists, then create it
			if (file.exists()) {
				file.delete();
			}
			file.createNewFile();

			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			bw.write(output);
		}finally{
			close(bw);
		}	
		

	}
	
	public static void createAndUpdateFile(final boolean replaceFile, final String fileLocation, final String output) {
		try {
			Path path = Paths.get(fileLocation);
			Path parentDir = path.getParent();
			if (!Files.exists(parentDir)){
				Files.createDirectories(parentDir);
			}			    
			
			if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
				Files.createFile(path);
			}else if(replaceFile){
				Files.delete(path);
				Files.createFile(path);
			}

			Files.write(path, output.getBytes(), StandardOpenOption.APPEND);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static boolean isFileExist(final String fileLocation){
		Path path = Paths.get(fileLocation);
		return Files.exists(path, LinkOption.NOFOLLOW_LINKS);
	}
	
	public static String getFileContent(final String fileLocation) throws IOException{
		Path path = Paths.get(fileLocation);
		return new String(readAllBytes(path));
	}
	
	public static Properties loadResourceProperties(final String propertyFile) throws IOException {
		Properties prop = new Properties();
		InputStream is = null;
		try{
			is = ClassLoader.getSystemResourceAsStream(propertyFile);
			prop.load(is);
		}finally{
			close(is);
		}
		
		System.out.println(prop.toString());
		return prop;

	}
	
	public static void close(Closeable closer){
		try{
			closer.close();
		}catch(Exception ex){
			
		}
	}

}
