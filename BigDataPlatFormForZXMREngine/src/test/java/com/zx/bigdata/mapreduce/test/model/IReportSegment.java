package com.zx.bigdata.mapreduce.test.model;

import com.zx.bigdata.bean.datadef.Segment;

/**
 * （1） 类型为AN或ANC的数据项是左对齐的，并在右面用空格补齐。
 * <p>
 * （2） 类型为N的数据项是右对齐的，并在左面用0补齐。
 * 
 * @author liuwu
 *
 */
public interface IReportSegment {

	public Segment getSegment(String segName);

}
