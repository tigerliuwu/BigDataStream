package com.zx.bigdata.mapreduce.bean;

public class JSONValueBuilder {
	
	private StringBuilder builder = new StringBuilder("{");
	
	public void appendKey(String key) {
		if (builder.length() > 1) {
			builder.append(",");
		}
		builder.append("\"").append(key).append("\":");
	}
	
	public void appendValue(String value) {
		builder.append("\"").append(value).append("\"");
	}
	
	public String getJSONString() {
		builder.append("}");
		return builder.toString();
	}
	
	public String toString() {
		return getJSONString();
	}

}
