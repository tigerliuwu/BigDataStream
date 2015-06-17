package org.wlu.spark.commons.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

public abstract class Configuration {
	
	protected String PROPERTIES = "conf.properties";
	
	
	private static Logger logger = Logger.getLogger(Configuration.class);
	
	public void loadFromProperties(Configuration conf) {
		loadFromProperties(conf, PROPERTIES);
	}
	
	public void loadFromProperties(Configuration conf, String f) {
		Properties properties = new Properties();
		try {
			logger.debug("load properties from file:" +f);
			InputStream is = this.getClass().getClassLoader().getResourceAsStream(f);
			
			properties.load(is);
			is.close();
			logger.info("load parameters:"+properties.toString());
			
			logger.info("load finish.");
			
			loadFromProperties(properties);
		} catch (IOException ex) {
			logger.warn("Unable to read properties file '" + f + "', skipping.", ex);
		}
	}
	
	public void loadFromProperties(Properties properties){
		setup((Map)properties);
	}
	
	public abstract void setup(Map<String, String> properties);
	
	public abstract void setProperties(String file);
}
