package com.zx.bigdata.bean.datadef.rules;

import com.zx.bigdata.bean.datadef.DataItem;
import com.zx.bigdata.bean.datadef.DataItemTypeEnum;

/**
 * 根据字段的校验规则对字段的值进行校验，所有的校验规则在RuleMethodEnum里面注册，其具体实现在RuleMethod类中 
 *
 */
public class RulesExecutor {
	private static RuleMethod ruleMethod = new RuleMethod();
	
	/**
	 * 对一个字段的值应用其所有的校验规则
	 * @param field	DataItem
	 * @param value	该字段对应的值 
	 * @return 所有的校验规则通过，返回true
	 * @throws Exception
	 */
	public static boolean executeRule(DataItem field, Object value) throws Exception {
		boolean result = true;
		
		//第一步：空校验
		if (value==null || "".equals(value.toString())) {
			for (Rule rul : field.getRules()) {
				if (RuleMethodEnum.IS_NULLABLE.getMethodName().equals(rul.getRuleName())) {
//					throw new IOException("nice to come here");
					return true;
				}
			}
//			throw new IOException("bad to come here");
			return false;
			
		}
		
		/**
		 * 后续的校验都是针对有值的校验
		 */
		
		// 第二步：类型校验
		DataItemTypeEnum fieldType = field.getDataItemType();
		result = fieldType.isValid(value.toString());
		
		if (result == false) {
			return result;
		}
		
		// 对其它的校验规则进行校验
		int startPos = -1;
		int endPos = -1;
		float startRange = 0;
		float endRange = 0;
		for (int i = 0; result && i < field.getRules().size(); i++) {
			Rule rule = field.getRules().get(i);
			switch(RuleMethodEnum.fromName(rule.getRuleName())) {
			case IS_NULLABLE:
				break;
			case CHECK_LENGTH:
				//第一个参数为该字段期望的长度
				int expectedLen = Integer.parseInt(rule.getParams().get(0));
				result = ruleMethod.checkLength(value, expectedLen);
				break;
			case GET_STR_BY_START_END:
				startPos = Integer.parseInt(rule.getParams().get(0));
				endPos = Integer.parseInt(rule.getParams().get(1));
				String expectedVal = rule.getParams().size()>2?rule.getParams().get(2):"";
				String subVal = ruleMethod.getStrByStartEnd(value, startPos, endPos);
				if (!expectedVal.equals(subVal)) {
					result = false;
				}
				break;
			case DATA_IN_RANGE:
				startRange = Float.parseFloat(rule.getParams().get(0));
				endRange = Float.parseFloat(rule.getParams().get(1));
				result = ruleMethod.dataInRange(value, startRange, endRange);
				break;
			case DATA_IN_SET:
				String dictionaryStr = rule.getParams().get(0);//取得枚举类型的数据字典的值（以，分隔）
				result = ruleMethod.dataInSet(value, dictionaryStr);
				break;
			case CHECK_DATE:
				String dateFormat = rule.getParams().get(0);
				result = ruleMethod.checkDate(value.toString(), dateFormat);
				break;
			case CHECK_ID_CARD_VALIDATE_CODE:
				result = ruleMethod.checkIDCardValidateCode(value.toString());
				break;
			case IS_VERSION_FORMAT:
				result = ruleMethod.isVersionFormat(value.toString());
			default:
				break;
			}
		}
		
		return result;
	}

}
