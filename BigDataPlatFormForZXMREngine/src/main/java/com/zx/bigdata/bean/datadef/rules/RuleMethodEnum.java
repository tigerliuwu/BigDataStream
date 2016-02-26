package com.zx.bigdata.bean.datadef.rules;

/**
 * 校验规则注册类，枚举了所有在RuleMethod里面实现的方法(或者规则)<p/>
 * <b>注意：</b>属性:methodName必须和RuleMethod里面的实现的方法名完全一致以及
 * 在此处注册后，在RulesExecutor里面对新注册的方法进行调用。
 *
 */
public enum RuleMethodEnum {
	
	IS_NULLABLE("isNullable"),
//	CHECK_DATA_TYPE("checkDataType",1),
	CHECK_LENGTH("checkLength",1),
	GET_STR_BY_START_END("getStrByStartEnd",2),
	DATA_IN_RANGE("dataInRange",2),
	DATA_IN_SET("dataInSet",1),
	CHECK_DATE("checkDate",1),
	CHECK_ID_CARD_VALIDATE_CODE("checkIDCardValidateCode"),
	IS_VERSION_FORMAT("isVersionFormat"),
	;
	
	private String methodName;
	private int numParas = 0;
	
	private RuleMethodEnum(String name) {
		this.methodName = name;
	}
	
	private RuleMethodEnum(String name, int num) {
		this.methodName = name;
		this.numParas = num;
	}
	
	public static RuleMethodEnum fromName(String name) throws Exception{
		for (RuleMethodEnum ruleMethod : values()) {
			if (ruleMethod.getMethodName().equals(name)) {
				return ruleMethod;
			}
		}
		
		throw new Exception("No such rule method, please check the name or define it");
	}
	
	public static boolean contains(String ruleName) {
		boolean result = false;
		try {
			fromName(ruleName);
			result = true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	public String getMethodName() {
		return this.methodName;
	}
	
	public int getParamsNum() {
		return this.numParas;
	}

}