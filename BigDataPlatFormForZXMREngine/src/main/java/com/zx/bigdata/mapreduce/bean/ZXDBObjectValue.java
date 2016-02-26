package com.zx.bigdata.mapreduce.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

@Deprecated
public class ZXDBObjectValue implements Writable {
	
	private String colName = null; // DBColumnObject's name
	private String value = null;
	
	public ZXDBObjectValue(String name,String val) {
		this.colName = name;
		this.value = val;
	}

	public void readFields(DataInput in) throws IOException {
		
		int length = in.readInt();
		if (length == -1) {
			this.colName = null;
		} else {
			byte[] byteVal = new byte[length];
			in.readFully(byteVal);
			this.colName = Bytes.toString(byteVal);
		}
		
		length = in.readInt();
		if (length == -1) {
			this.value = null;
		} else {
			byte[] byteVal = new byte[length];
			in.readFully(byteVal);
			this.value = Bytes.toString(byteVal);
		}

	}

	public void write(DataOutput out) throws IOException {
		
		if (this.colName == null) {
			out.writeInt(-1);
		} else {
			byte[] byteVal = Bytes.toBytes(this.colName);
			out.writeInt(byteVal.length);
			out.write(byteVal);
		}
		
		if (this.value == null) {
			out.writeInt(-1);
		} else {
			byte[] byteVal = Bytes.toBytes(this.value);
			out.writeInt(byteVal.length);
			out.write(byteVal);
		}

	}
	
	public int hashCode() {
		final int prime = 31;
		int result  = 1;
		
		result = result * prime + (this.colName==null?0:this.colName.hashCode());
		result = result * prime + (this.value==null?0:this.value.hashCode());
		
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
		
		ZXDBObjectValue pairObj = (ZXDBObjectValue)obj;

		if (this.colName == null) {
			if (pairObj.colName !=null) {
				return false;
			}
		} else if (!this.colName.equals(pairObj.colName)) {
			return false;
		}
		
		if (this.value == null) {
			if (pairObj.value !=null) {
				return false;
			}
		} else if (!this.value.equals(pairObj.value)) {
			return false;
		}
		
		return true;
		
	}

}
