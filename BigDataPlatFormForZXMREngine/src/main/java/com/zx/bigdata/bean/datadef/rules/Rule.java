package com.zx.bigdata.bean.datadef.rules;

import java.util.ArrayList;
import java.util.List;

/**
 * 定义字段的校验规则
 * @author liuwu
 *
 */
public class Rule {
	
	private String ruleName;	// 规则名称
	private List<String> params = new ArrayList<String>();
	
	public Rule(){
		
	}
	
	public Rule(String ruleName) {
		this.ruleName  =  ruleName;
	}
	/**
	 * @return the ruleName
	 */
	public String getRuleName() {
		return ruleName;
	}
	/**
	 * @param ruleName the ruleName to set
	 */
	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}
	/**
	 * @return the params
	 */
	public List<String> getParams() {
		return params;
	}
	/**
	 * @param params the params to set
	 */
	public void setParams(List<String> params) {
		this.params = params;
	}
	
	
}
