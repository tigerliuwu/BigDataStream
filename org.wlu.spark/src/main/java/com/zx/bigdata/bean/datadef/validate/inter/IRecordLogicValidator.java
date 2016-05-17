package com.zx.bigdata.bean.datadef.validate.inter;

import com.zx.bigdata.bean.feedback.ZXValidatorFeedBack;
import com.zx.bigdata.mapreduce.bean.ReportRecord;

public interface IRecordLogicValidator {
	public ZXValidatorFeedBack validateRecord(ReportRecord record, ZXValidatorFeedBack retVal);

}
