package com.zx.bigdata.bean.datadef.validate.lib;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.zx.bigdata.bean.datadef.DataItemTypeEnum;
import com.zx.bigdata.bean.datadef.rules.ZXValidatorPrototype;
import com.zx.bigdata.bean.datadef.rules.vo.RuleEntity;
import com.zx.bigdata.bean.datadef.validate.dict.DictCache;
import com.zx.bigdata.bean.datadef.validate.inter.IZXValidator;
import com.zx.bigdata.bean.feedback.ZXValidatorFeedBack;

public class SolidValidatorLib implements IZXValidator {

	enum RuleMethodEnum {

		IS_NULLABLE("isNullable"), // 空校验
		IS_NOTNULLABLE("isNotNullable"), // 非空校验
		DATA_TYPE_CHECK("dataTypeCheck"), // 数据类型校验
		DICTIONARY_CHECK("dictionaryCheck"), // 数据字典校验
		CHECK_LENGTH("checkLength"), // 长度校验
		GET_STR_BY_START_END("getStrByStartEnd"), // 获取字符串的起始终止时间
		DATA_IN_RANGE("dataInRange"), // 数据范围校验
		DATA_IN_SET("dataInSet"), // 数据所在的集合校验（数据字典的补充部分）
		CHECK_DATE("checkDate"), // 日期校验
		CHECK_ID_CARD_VALIDATE_CODE("checkIDCardValidateCode"), // 身份证校验
		IS_VERSION_FORMAT("isVersionFormat"); // 版本类型校验

		private String methodName;

		private RuleMethodEnum(String name) {
			this.methodName = name;
		}

		public static RuleMethodEnum fromName(String name) {
			for (RuleMethodEnum ruleMethod : values()) {
				if (ruleMethod.methodName.equals(name)) {
					return ruleMethod;
				}
			}

			throw new RuntimeException("No such rule method:\"" + name + "\", please check the name or define it");
		}
	}

	private Map<String, ZXValidatorPrototype> validators = null;

	public SolidValidatorLib() {
		validators = new HashMap<String, ZXValidatorPrototype>();
	}

	public ZXValidatorPrototype getValidator(String ruleName) {
		ZXValidatorPrototype validator = validators.get(ruleName);
		if (validator != null) {
			return validator;
		}
		switch (RuleMethodEnum.fromName(ruleName)) {
		case IS_NULLABLE:
			validator = new NullableValidator();
			break;
		case IS_NOTNULLABLE:
			validator = new NotNullableValidator();
			break;
		case DATA_TYPE_CHECK:
			validator = new DataTypeCheckValidator();
			break;
		case DICTIONARY_CHECK:
			validator = new DictionaryCheckValidator();
			break;
		case CHECK_LENGTH:
			// 第一个参数为该字段期望的长度
			validator = new CheckLengthValidator();
			break;
		case GET_STR_BY_START_END:
			// TODO
			validator = new ZXValidatorPrototype();
			break;
		case DATA_IN_RANGE:
			// TODO
			validator = new ZXValidatorPrototype();
			break;
		case DATA_IN_SET:
			// TODO
			validator = new ZXValidatorPrototype();
			break;
		case CHECK_DATE:
			// TODO
			validator = new ZXValidatorPrototype();
			break;
		case CHECK_ID_CARD_VALIDATE_CODE:
			// TODO
			validator = new ZXValidatorPrototype();
			break;
		case IS_VERSION_FORMAT:
			// TODO
			validator = new ZXValidatorPrototype();
			break;
		default:
			throw new RuntimeException("no such rule:" + ruleName);
		}
		validators.put(ruleName, validator);
		return validator;
	}

	class NullableValidator extends ZXValidatorPrototype {
		public ZXValidatorFeedBack validateField(RuleEntity ruleField, ZXValidatorFeedBack retVal) {
			/*
			 * if (ruleField.getParams() == null || ruleField.getParams().size()
			 * == 0) { return retVal; }
			 */

			return retVal;

		}
	}

	class NotNullableValidator extends ZXValidatorPrototype {
		public ZXValidatorFeedBack validateField(RuleEntity ruleField, ZXValidatorFeedBack retVal) {

			if (ruleField.getValue() == null || ruleField.getValue().isEmpty()) {
				retVal.add(ruleField.getColName(), "unknownCode");
			}
			return retVal;
		}
	}

	class DataTypeCheckValidator extends ZXValidatorPrototype {
		public ZXValidatorFeedBack validateField(RuleEntity ruleField, ZXValidatorFeedBack retVal) {

			boolean isValid = isValid(ruleField.getDataType(), ruleField.getValue());
			if (!isValid) {
				retVal.add(ruleField.getColName(), "unknownCode");
			}

			return retVal;
		}

		public boolean isValid(DataItemTypeEnum dataType, String value) {

			boolean result = false;
			try {
				switch (dataType) {
				case N:// N
					Long.parseLong(value.trim());
					result = true;
					break;
				case AN: // AN
					int i = 0;
					for (; i < value.length(); i++) {
						char c = value.charAt(i);
						if (c >= '0' && c <= '9') {

						} else if (c >= 0X20 && c <= 0X7E) {

						} else {
							break;
						}
					}
					if (i == value.length()) {
						result = true;
					}
					break;
				case ANC: // ANC
					result = true;
					break;
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				result = false;
			}

			return result;
		}
	}

	class DictionaryCheckValidator extends ZXValidatorPrototype {
		public ZXValidatorFeedBack validateField(RuleEntity ruleField, ZXValidatorFeedBack retVal) {

			// TODO 需要额外实现一个接口，传入字段的标识符和对应的值进行字典的查找和校验
			Collection<String> set = DictCache.getFieldBy(ruleField.getColName());
			if (!set.contains(ruleField.getValue())) {// 字段对应的值在数据字典中不存在
				retVal.add(ruleField.getColName(), "unknowncode");
			}

			return retVal;
		}
	}

	class CheckLengthValidator extends ZXValidatorPrototype {
		public ZXValidatorFeedBack validateField(RuleEntity ruleField, ZXValidatorFeedBack retVal) {

			int expectedLen = Integer.parseInt(ruleField.getParams().get(0));
			if (ruleField.getValue() == null || ruleField.getValue().isEmpty()) {
				if (expectedLen == 0) {
					retVal.add(ruleField.getColName(), "unknowncode");
				}
			} else {
				if (ruleField.getValue().length() != expectedLen) {
					retVal.add(ruleField.getColName(), "unknowncode");
				}
			}

			return retVal;
		}
	}
}
