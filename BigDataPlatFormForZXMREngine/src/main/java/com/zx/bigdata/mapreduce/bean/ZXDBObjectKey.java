package com.zx.bigdata.mapreduce.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

public class ZXDBObjectKey implements WritableComparable<ZXDBObjectKey> {
	
	public String key = null;
	public boolean secondaryKey = false; //是否为二级索引
	
	public ZXDBObjectKey(String k) {
		this(k, false);
	}
	
	public ZXDBObjectKey(String k, boolean isSKey) {
		this.key = k;
		this.secondaryKey = isSKey;
	}
	
	public int hashCode() {
		final int prime = 31;
		int result  = 1;
		
		result = result * prime + (this.key==null? 0:this.key.hashCode());
		result = result + (this.secondaryKey?1:0);
		
		return result;
	}
	
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (obj == null) {
			return false;
		}
		
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		ZXDBObjectKey pairObj = (ZXDBObjectKey)obj;
		
		if (this.secondaryKey != pairObj.secondaryKey) {
			return false;
		}
		
		if (this.key == null) {
			if (pairObj.key !=null) {
				return false;
			}
		} else if (!this.key.equals(pairObj.key)) {
			return false;
		}
		
		return true;
		
	}
	
	public void readFields(DataInput in) throws IOException {
		this.secondaryKey = in.readBoolean();
		int length = in.readInt();
		if (length == -1) {
			this.key = null;
		} else {
			byte[] byteKey = new byte[length];
			in.readFully(byteKey);
			this.key = Bytes.toString(byteKey);
		}

	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(this.secondaryKey);
		if (this.key == null) {
			out.writeInt(-1);
		} else {
			byte[] byteKey = Bytes.toBytes(this.key);
			out.writeInt(byteKey.length);
			out.write(byteKey);
		}
		
	}

	public int compareTo(ZXDBObjectKey other) {
		int result = 0;
		
		if (other == null) {
			result = -1;
		} else {
			result = this.key.compareTo(other.key);
		}
				
		return result;
	}
	
	
	
}
