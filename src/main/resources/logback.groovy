import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.FileAppender;

import static ch.qos.logback.classic.Level.DEBUG;
import static ch.qos.logback.classic.Level.INFO;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.LinkOption;
import ch.qos.logback.classic.AsyncAppender;


// always a good idea to add an on console status listener
//statusListener(OnConsoleStatusListener)


// define the USER_HOME variable setting its value 
// to that of the "user.home" system property

def USER_HOME = System.getProperty("user.home");
def env = System.getenv();
def kafkaLogDir = System.getenv('KAFKA_CONNECT_LOG');
def kafkaConfigDir = System.getenv('KAFKA_CONNECT_CONFIG');
def kafkaDefaultLogDir = "/kafka_connect_log";
def kafkaConfFileName = "/connector.properties";

if(kafkaLogDir == null){
    kafkaLogDir = USER_HOME + kafkaDefaultLogDir
}

if(kafkaConfigDir == null){
    println "log file directory doesn't configured"
    kafkaConfigDir = USER_HOME + "/kafka_connect_config"
}

//reading property file
Path path = Paths.get(kafkaConfigDir+kafkaConfFileName) 

// If it doesn't exist
Path parentDir = path.getParent();
if (!Files.exists(parentDir)){
    println "config directory doesn't exist"
    Files.createDirectories(parentDir);
}

if( !Files.exists(path,LinkOption.NOFOLLOW_LINKS) ) {
    println "config file doesn't exist"
  Files.createFile(path);
}

Properties properties = new Properties()
File propertiesFile = new File(kafkaConfigDir+kafkaConfFileName)
propertiesFile.withInputStream {
    properties.load(it)
}

properties.each { k,v->
    println "$k = $v"
}

def maxHistoryValue = 30;
def totalSizeCapValue = "30gb";
def queueSizeValue = 500;

def maxHistoryString = "maxHistory"
def totalSizeCapString = "totalSizeCap";
def queueSize = "queueSize";

if(properties."totalSizeCap" != null){
    totalSizeCapValue = properties."totalSizeCap";
}

if(properties."maxHistory" != null){
    maxHistoryValue = (properties."maxHistory").toInteger();
}

if(properties."queueSize" != null){
    queueSizeValue = (properties."queueSize").toInteger();
}


println "USER_HOME=${USER_HOME}"
println  "KAFKA_CONNECT_LOG=${kafkaLogDir}"
println  "KAFKA_CONNECT_CONFIG=${kafkaConfigDir}"
println  "maxHistory=${maxHistoryValue}"
println  "totalSizeCap=${totalSizeCapValue}"

appender("FILE", RollingFileAppender) {
  println "Setting [file] property to [${kafkaLogDir}/logFile.log]"
  file = "${kafkaLogDir}/logFile.log"  
  rollingPolicy(TimeBasedRollingPolicy) {
    fileNamePattern = "${kafkaLogDir}/logFile-%d{yyyy-MM-dd}.log"
    maxHistory = maxHistoryValue
    totalSizeCap = FileSize.valueOf(totalSizeCapValue);
  }
  encoder(PatternLayoutEncoder) {
    pattern = "%date [%thread] %-5level %ex{full} %logger - %msg%n"
  }
}

appender("ASYNC", AsyncAppender) {
  discardingThreshold=0;
  queueSize=queueSizeValue;
  neverBlock=true;
  appenderRef("FILE");
}

//root(DEBUG, ["ASYNC"])
root(INFO, ["ASYNC"])

logger("com.pearson.gme.mydashboard", DEBUG,["FILE"],false)
//logger("io.netty.handler.logging.LoggingHandler", INFO,["FILE"])

//Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
//root.setLevel(Level.INFO);