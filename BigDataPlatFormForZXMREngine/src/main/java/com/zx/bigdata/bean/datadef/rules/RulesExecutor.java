package com.zx.bigdata.bean.datadef.rules;

import com.zx.bigdata.bean.datadef.DataItem;
import com.zx.bigdata.bean.datadef.rules.vo.RuleEntity;
import com.zx.bigdata.bean.datadef.validate.lib.SolidValidatorLib;
import com.zx.bigdata.bean.feedback.ZXValidatorFeedBack;

/**
 * 根据字段的校验规则对字段的值进行校验，所有的校验规则在RuleMethodEnum里面注册，其具体实现在RuleMethod类中
 *
 */
public class RulesExecutor {
	private static SolidValidatorLib validatorFactory = new SolidValidatorLib();

	/**
	 * 对一个字段的值应用其所有的校验规则
	 * 
	 * @param field
	 *            DataItem
	 * @param value
	 *            该字段对应的值
	 * @return 所有的校验规则通过，返回true
	 * @throws Exception
	 */
	public static boolean executeRule(DataItem field, Object value, ZXValidatorFeedBack feedback) {

		int first = feedback.getAll().size();

		// 第一步：空校验
		if (value == null || "".equals(value.toString())) {
			for (Rule rul : field.getRules()) {
				if (RuleMethodEnum.IS_NULLABLE.getMethodName().equals(rul.getRuleName())) {
					// throw new IOException("nice to come here");
					return true;
				}
			}
			// throw new IOException("bad to come here");
			return false;

		}

		/**
		 * 后续的校验都是针对有值的校验
		 */

		// 第二步：类型校验
		RuleEntity ruleEntity = new RuleEntity();
		ruleEntity.setColName(field.getName());
		ruleEntity.setValue(value.toString());
		ruleEntity.setDataType(field.getDataItemType());
		ruleEntity.setRuleName("dataTypeCheck");
		ruleEntity.setParams(null);
		ZXValidatorPrototype validator = validatorFactory.getValidator(ruleEntity.getRuleName());
		validator.validateField(ruleEntity, feedback);

		// 对其它的校验规则进行校验
		for (int i = 0; !feedback.isFull() && i < field.getRules().size(); i++) {
			Rule rule = field.getRules().get(i);
			ruleEntity = new RuleEntity();
			ruleEntity.setColName(field.getName());
			ruleEntity.setValue(value.toString());
			ruleEntity.setDataType(field.getDataItemType());
			ruleEntity.setRuleName(rule.getRuleName());
			ruleEntity.setParams(rule.getParams());
			validator = validatorFactory.getValidator(ruleEntity.getRuleName());

			validator.validateField(ruleEntity, feedback);
		}

		return first == feedback.getAll().size();
	}

}
