package com.zx.bigdata.mapreduce.test.tax.bean;

import java.util.Map;

import com.zx.bigdata.bean.datadef.DataSchema;
import com.zx.bigdata.bean.datadef.ReportTypeEnum;
import com.zx.bigdata.bean.datadef.Segment;
import com.zx.bigdata.bean.datadef.SourceFileTypeEnum;

public class TaxDataSchema {

	DataSchema dataSchema;

	private void init() {
		this.dataSchema = new DataSchema();
		this.dataSchema.setFileType(SourceFileTypeEnum.Plain);
		this.dataSchema.setReportInformationType("D"); // 税务
		this.dataSchema.setName("个人税务");
	}

	public TaxDataSchema(ReportTypeEnum type) {
		init();
		this.dataSchema.setReportType(type);
	}

	public void setSegments(Map<String, Segment> segments) {
		this.dataSchema.setSegments(segments);
	}

	public void addSegment(Segment seg) {
		this.dataSchema.getSegments().put(seg.getName(), seg);
	}

	public DataSchema getDataSchema() {
		return this.dataSchema;
	}

}
