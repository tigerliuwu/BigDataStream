package com.zx.bigdata.bean.datadef;

public enum DataItemTypeEnum {

	N, // 数值型
	AN, // 数字0-9 或者 ASCII范围为:0X20-0X7E
	/**
	 * 双字节1区 A1A1-A9FE 图形符号
	 * 
	 */
	ANC;

	public static DataItemTypeEnum fromName(String fieldType) throws Exception {
		for (DataItemTypeEnum field : values()) {
			if (field.name().equalsIgnoreCase(fieldType)) {
				return field;
			}
		}

		StringBuilder builder = new StringBuilder();
		for (DataItemTypeEnum field : values()) {
			builder.append(field.name()).append(",");
		}

		throw new Exception("No such Field Type:" + fieldType.toUpperCase() + " so far, only support these fieldTypes:"
				+ builder.substring(0, builder.length() - 1));
	}

}
