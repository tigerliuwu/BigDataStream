package com.zx.bigdata.bean.datadef;

/**
 * 定义HDFS数据源文件的数据格式，目前只支持xml和plain两种格式
 * @author liuwu
 *
 */
public enum SourceFileTypeEnum {
	
	XML,
	Plain	//以字节进行分割的文件

}
