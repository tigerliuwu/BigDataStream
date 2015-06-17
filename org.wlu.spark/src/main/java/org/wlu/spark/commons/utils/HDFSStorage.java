package org.wlu.spark.commons.utils;

import java.util.Map;
import java.util.Properties;

public class HDFSStorage extends Configuration {
	
	private enum HDFSKEYS {
		NAMENODEURI,
		USERNAME,
		PROPERTIES
	}
	
	public String getNameNodeUri() {
		return nameNodeUri;
	}
	
	private String nameNodeUri;
	private String username;
	private Properties properties = new Properties();

	public void setNameNodeUri(String nameNodeUri) {
		this.nameNodeUri = nameNodeUri;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	public void setPropertiesPros(String key, String value) {
		this.properties.setProperty(key, value);
	}

	public void setup(Map confProperties) {
		
		for (Object key : confProperties.keySet()) {
			if (HDFSKEYS.NAMENODEURI.toString().equals(key)) {
				setNameNodeUri(confProperties.get(HDFSKEYS.NAMENODEURI.toString()).toString());
			} else if (HDFSKEYS.USERNAME.toString().equals(key)) {
				setUsername(confProperties.get(HDFSKEYS.USERNAME.toString()).toString());
			} else {
				setPropertiesPros(key.toString(), confProperties.get(key).toString());
			}
		}
	}

	public void setProperties(String file) {
		this.PROPERTIES = file;
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(HDFSKEYS.NAMENODEURI.toString());
		builder.append("=");
		builder.append(this.nameNodeUri);
		builder.append("\t");
		builder.append(HDFSKEYS.USERNAME.toString());
		builder.append("=");
		builder.append(this.username);
		builder.append("\t");
		builder.append(this.properties.toString());
		
		return builder.toString();
	}
	
	public static void main(String[] args) {
		HDFSStorage hdfs = new HDFSStorage();
		hdfs.loadFromProperties(hdfs);
		
		System.out.println(hdfs.toString());
	}

}
