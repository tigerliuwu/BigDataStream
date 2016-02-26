package com.zx.bigdata.bean.datadef;

public enum DataItemTypeEnum {
	
	N,	// 数值型
	AN,	// 数字0-9 或者 ASCII范围为:0X20-0X7E
	/**
	 * 双字节1区	A1A1-A9FE	图形符号
	 * 
	 */
	ANC;
	
	public boolean isValid(String value) {
		
		boolean result = false;
		try {
			switch(this) {
			case N:// N
				Long.parseLong(value.trim());
				result = true;
				break;
			case AN: // AN
				int i = 0;
				for (; i < value.length(); i++) {
					char c = value.charAt(i);
					if (c >= '0' && c <= '9') {
						
					} else if (c >= 0X20 && c <= 0X7E) {
						
					} else {
						break;
					}
				}
				if (i == value.length()) {
					result = true;
				}
				break;
			case ANC:	// ANC
				result = true;
				break;
			}
		}catch(Exception ex) {
			ex.printStackTrace();
			result = false;
		} 
		
		return result;
	}
	
	public static DataItemTypeEnum fromName(String fieldType) throws Exception {
		for (DataItemTypeEnum field: values()) {
			if (field.name().equalsIgnoreCase(fieldType)) {
				return field;
			}
		}
		
		StringBuilder builder = new StringBuilder();
		for (DataItemTypeEnum field: values()) {
			builder.append(field.name()).append(",");
		}
		
		throw new Exception("No such Field Type:"+fieldType.toUpperCase()
				+" so far, only support these fieldTypes:"+builder.substring(0,builder.length()-1));
	}
	
}
