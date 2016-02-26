package com.zx.bigdata.mapreduce.bean;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 
 * @author liuwu
 *
 */
public class MRCounterMap extends AbstractMap<StatusCounterEnum, Counter> {
	
	private Map<StatusCounterEnum, Counter> counterMap = new HashMap<StatusCounterEnum, Counter>();
	private TaskAttemptContext context = null;
	
	public MRCounterMap(TaskAttemptContext ctx) {
		this.context = ctx;
	}
	
	@Override
	public Set<java.util.Map.Entry<StatusCounterEnum, Counter>> entrySet() {
		// TODO Auto-generated method stub
		return counterMap.entrySet();
	}
	
	public Counter put(StatusCounterEnum key, Counter value) {
		return this.counterMap.put(key, value);		
	}
	
	public Counter get(StatusCounterEnum key) {
		Counter value = this.counterMap.get(key);
		if (value== null) {
			value = this.put(key,this.context.getCounter(key));
		}
		return value;
	}

}
