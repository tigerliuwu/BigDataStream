package com.zx.bigdata.bean.datadef.rules.vo;

import java.util.ArrayList;
import java.util.List;

import com.zx.bigdata.bean.datadef.DataItemTypeEnum;

/**
 * 该类用于存储
 * 
 * @author liuwu
 *
 */
public class RuleEntity {

	private String colName; // 列名
	private String value; // 该列对应的值
	private DataItemTypeEnum dataType;

	private String ruleName; // 规则名称
	private List<String> params = new ArrayList<String>();// 应用上述校验规则需要的参数

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public DataItemTypeEnum getDataType() {
		return dataType;
	}

	public void setDataType(DataItemTypeEnum dataType) {
		this.dataType = dataType;
	}

	public String getRuleName() {
		return ruleName;
	}

	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}

	public List<String> getParams() {
		return params;
	}

	public void setParams(List<String> params) {
		this.params = params;
	}

}
