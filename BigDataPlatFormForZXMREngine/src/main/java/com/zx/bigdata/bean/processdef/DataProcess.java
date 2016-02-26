package com.zx.bigdata.bean.processdef;

import java.util.ArrayList;
import java.util.List;


/**
 * 代表一条数据流程
 * @author liuwu
 *
 */
public class DataProcess {
	
	private String name;	//数据流程名称
	/**
	 * 日期格式为：yyyyMMdd HH:mm:ss
	 */
	private String lastModified;	//上次数据流程修改时间(如果第一次创建，则和创建时间相同)
	private String lastModifier;	//上次数据流程修改者
	
	private List<String> hdfsPaths = new ArrayList<String>();	//定义该业务的HDFS数据源
	private String reportInformationType;	//该数据流程所使用的数据模型DataSchema的报文信息类别名称
	private List<DBSchema> dbSchemas = new ArrayList<DBSchema>();	// 一条报文记录写入HBase的rowkey和该rowkey对应值的构成信息
	private List<SecondaryIndex> sKeys = new ArrayList<SecondaryIndex>();	// 与该数据流程挂钩的所有的二级索引
	
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the lastModified
	 */
	public String getLastModified() {
		return lastModified;
	}
	/**
	 * @param lastModified the lastModified to set
	 */
	public void setLastModified(String lastModified) {
		this.lastModified = lastModified;
	}
	/**
	 * @return the lastModifier
	 */
	public String getLastModifier() {
		return lastModifier;
	}
	/**
	 * @param lastModifier the lastModifier to set
	 */
	public void setLastModifier(String lastModifier) {
		this.lastModifier = lastModifier;
	}

	/**
	 * @return the hdfsPaths
	 */
	public List<String> getHdfsPaths() {
		return hdfsPaths;
	}

	/**
	 * @param hdfsPaths the hdfsPaths to set
	 */
	public void setHdfsPaths(List<String> hdfsPaths) {
		this.hdfsPaths = hdfsPaths;
	}
	@Override
	public String toString() {
		String temp = hdfsPaths.toString();
		String result =temp.substring(1, temp.length()-1);
		return result;
	}
	/**
	 * @return the reportInformationType
	 */
	public String getReportInformationType() {
		return reportInformationType;
	}
	/**
	 * @param reportInformationType the reportInformationType to set
	 */
	public void setReportInformationType(String reportInformationType) {
		this.reportInformationType = reportInformationType;
	}
	/**
	 * @return the dbSchemas
	 */
	public List<DBSchema> getDbSchemas() {
		return dbSchemas;
	}
	/**
	 * @param dbSchemas the dbSchemas to set
	 */
	public void setDbSchemas(List<DBSchema> dbSchemas) {
		this.dbSchemas = dbSchemas;
	}
	/**
	 * @return the sKeys
	 */
	public List<SecondaryIndex> getsKeys() {
		return sKeys;
	}
	/**
	 * @param sKeys the sKeys to set
	 */
	public void setsKeys(List<SecondaryIndex> sKeys) {
		this.sKeys = sKeys;
	}

}
