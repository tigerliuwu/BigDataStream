package com.zx.bigdata.bean.datadef.validate.inter;

import com.zx.bigdata.bean.datadef.rules.vo.RuleEntity;
import com.zx.bigdata.bean.feedback.ZXValidatorFeedBack;

public interface IDataItemValidator {
	/**
	 * params[0] to store to value of the field, other params[1...n] to store
	 * the other parameters
	 * 
	 * @param params
	 * @param retVal
	 * @return
	 */
	public ZXValidatorFeedBack validateField(RuleEntity ruleField, ZXValidatorFeedBack retVal);

}
