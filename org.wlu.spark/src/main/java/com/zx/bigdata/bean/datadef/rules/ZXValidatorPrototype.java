package com.zx.bigdata.bean.datadef.rules;

import com.zx.bigdata.bean.datadef.rules.vo.RuleEntity;
import com.zx.bigdata.bean.datadef.validate.inter.IDataItemValidator;
import com.zx.bigdata.bean.datadef.validate.inter.IRecordLogicValidator;
import com.zx.bigdata.bean.feedback.ZXValidatorFeedBack;
import com.zx.bigdata.mapreduce.bean.ReportRecord;

public class ZXValidatorPrototype implements IRecordLogicValidator, IDataItemValidator {

	public ZXValidatorFeedBack validateRecord(ReportRecord record, ZXValidatorFeedBack retVal) {

		/*
		 * if (retVal == null) { retVal = new ZXValidatorFeedBack(); }
		 * 
		 * if (retVal.isFull()) { return retVal; }
		 */

		throw new RuntimeException("need to implement the method:validate.");

		/*
		 * if (!retVal.isFull()) { validateDict(record, entity.dictFields,
		 * retVal); }
		 * 
		 * if (!retVal.isFull()) { this.validateLogic(record,
		 * entity.logicFields, retVal); }
		 */

	}

	public ZXValidatorFeedBack validateField(RuleEntity ruleField, ZXValidatorFeedBack retVal) {
		/*
		 * if (ruleField.getParams() == null || ruleField.getParams().size() ==
		 * 0) { return retVal; }
		 */
		// TODO

		throw new RuntimeException("need to implement the method:validate.");
	}

	/**
	 * 考虑到
	 * 
	 * @param record
	 * @param dictFields
	 * @param retVal
	 * @return
	 */
	/*
	 * protected ZXValidatorFeedBack validateDict(ReportRecord record,
	 * SegmentDataItemPair[] dictFields, ZXValidatorFeedBack retVal) { if
	 * (dictFields == null || dictFields.length == 0) { return retVal; } else {
	 * throw new RuntimeException(
	 * "Need to overwrite method:\"validateDict\" since there are dictionary fields to validate."
	 * ); }
	 * 
	 * }
	 * 
	 * protected ZXValidatorFeedBack validateLogic(ReportRecord record,
	 * SegmentDataItemPair[] logicFields, ZXValidatorFeedBack retVal) { if
	 * (logicFields == null || logicFields.length == 0) { return retVal; } else
	 * { throw new RuntimeException(
	 * "Need to overwrite method:\"validateLogic\" since there are logic fields to validate."
	 * ); }
	 * 
	 * }
	 */

}
