package com.zx.bigdata.bean.datadef;

import java.util.HashMap;
import java.util.Map;

/**
 * 该类的一个实例代表一个数据模型
 * 
 * @author liuwu
 *
 */
public class DataSchema {

	private String name; // 数据模型的名称
	private String reportInformationType; // 报文信息类别，在保存该实例时，作为Hbase rowkey的开始部分
	private String lastModified; // 数据模型上次修改时间，在保存该实例时，作为Hbase rowkey的结束部分
	private String lastModifier; // 数据模型上次修改者
	private SourceFileTypeEnum fileType; // 表示该报文的源文件的文件格式：xml,plain
	private ReportTypeEnum reportType = ReportTypeEnum.NORMAL; // 报文类型：normal(正常报文),delete（删除报文）,feedback（反馈报文）
	/**
	 * 代表报文一条记录中所有的数据字段。其中key为信息类别，每个key对应的值为该信息段所有数据字段的集合。
	 * 当key为(“basic”)时，对应的信息段为基本信息段或者该记录只有一个信息段;当key为(“head”)时，表示为报文头
	 */
	private Map<String, Segment> segments = new HashMap<String, Segment>();

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the reportInformationType
	 */
	public String getReportInformationType() {
		return reportInformationType;
	}

	/**
	 * @param reportInformationType
	 *            the reportInformationType to set
	 */
	public void setReportInformationType(String reportInformationType) {
		this.reportInformationType = reportInformationType;
	}

	/**
	 * @return the lastModified
	 */
	public String getLastModified() {
		return lastModified;
	}

	/**
	 * @param lastModified
	 *            the lastModified to set
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
	 * @param lastModifier
	 *            the lastModifier to set
	 */
	public void setLastModifier(String lastModifier) {
		this.lastModifier = lastModifier;
	}

	/**
	 * @return the fileType
	 */
	public SourceFileTypeEnum getFileType() {
		return fileType;
	}

	/**
	 * @param fileType
	 *            the fileType to set
	 */
	public void setFileType(SourceFileTypeEnum fileType) {
		this.fileType = fileType;
	}

	/**
	 * @return the reportType
	 */
	public ReportTypeEnum getReportType() {
		return reportType;
	}

	/**
	 * @param reportType
	 *            the reportType to set
	 */
	public void setReportType(ReportTypeEnum reportType) {
		this.reportType = reportType;
	}

	/**
	 * @return the segments
	 */
	public Map<String, Segment> getSegments() {
		return segments;
	}

	/**
	 * @param segments
	 *            the segments to set
	 */
	public void setSegments(Map<String, Segment> segments) {
		this.segments = segments;
	}
}
