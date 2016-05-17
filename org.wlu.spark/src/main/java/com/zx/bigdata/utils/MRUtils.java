package com.zx.bigdata.utils;

public class MRUtils {
	public final static String KEY_SPLIT_DEL = "#"; // 用来分隔关键字段

	public final static String HBASE_TABLE_FAMILY = "f";
	public final static String HBASE_RECORD_STATUS_COL = "status";

	public final static String HBASE_PROCESS_DATA = "businessData"; // 数据流程执行中数据写入表

	public final static String HBASE_PROCESS_SNDINDEX_DATA = "businessSKData"; // 数据流程执行中二级索引写入表

	public final static String HBASE_PROCESS_SNDINDEX_VAL_COL = "v"; // 二级索引表中值的列名

	public final static String DATE_PATTERN = "yyyyMMdd HH:mm:ss";

	public final static String FIELD_DELIMITER = ";";

	public final static String CONSTANT_DELETE = "DEL"; // 在删除报文的时候使用，追加在删除报文流程中定义的字段的后面(#DEL)

	public final static String CONSTANT_DELETE_STATUS = "1";

	public final static String ENCODING_UTF8 = "UTF-8";
}
