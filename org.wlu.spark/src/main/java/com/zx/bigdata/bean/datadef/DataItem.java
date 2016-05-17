package com.zx.bigdata.bean.datadef;

import java.util.ArrayList;
import java.util.List;

import com.zx.bigdata.bean.datadef.rules.Rule;

/**
 * 表示一个数据字段
 * @author liuwu
 *
 */
public class DataItem {

	private String name;	//数据字段的列名
	
	private DataItemTypeEnum dataItemType;	//字段的类型，目前只有N,AN,ANC三种
	
	private String tag; //当报文为xml文件格式时，该字段为必须项，表示xml文件中一个元素名。
	
	private int startPose; // 当报文为Plain文件格式时，该字段为必须项，表示该字段的起始位置。(根据接口文档，起始位置为1)
	
	private int endPose; // 当报文为Plain文件格式时，该字段为必须项，表示该字段的结束位置。
	
	private List<Rule> rules = new ArrayList<Rule>();	// 该字段所有的校验规则集合
	

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	/**
	 * @return the rules
	 */
	public List<Rule> getRules() {
		return rules;
	}

	/**
	 * @param rules the rules to set
	 */
	public void setRules(List<Rule> rules) {
		this.rules = rules;
	}
	

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the dataItemType
	 */
	public DataItemTypeEnum getDataItemType() {
		return dataItemType;
	}

	/**
	 * @param dataItemType the dataItemType to set
	 */
	public void setDataItemType(DataItemTypeEnum dataItemType) {
		this.dataItemType = dataItemType;
	}

	/**
	 * @return the startPose
	 */
	public int getStartPose() {
		return startPose;
	}

	/**
	 * @param startPose the startPose to set
	 */
	public void setStartPose(int startPose) {
		this.startPose = startPose;
	}

	/**
	 * @return the endPose
	 */
	public int getEndPose() {
		return endPose;
	}

	/**
	 * @param endPose the endPose to set
	 */
	public void setEndPose(int endPose) {
		this.endPose = endPose;
	}
	
	@Override
	public String toString() {
		return "DataField [name=" + name + ", dataType=" + dataItemType + ", tag=" + tag + ", startPose="
				+ startPose + ", endPose=" + endPose + "]";
	}


    public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
    	if (obj instanceof DataItem) {

			DataItem other = (DataItem)obj;
			if (!this.name.equals(other.name)) {
				return false;
			}
			if (!(dataItemType == other.dataItemType)) {
				return false;
			}
			if (!(tag == other.tag || tag.equals(other.tag))) {
				return false;
			}
			if (!(startPose == other.startPose)) {
				return false;
			}
			if (!(endPose == other.endPose)) {
				return false;
			}
			if (rules.size() != other.rules.size()) {
				return false;
			}
			for (int i = 0; i < rules.size(); i++) {
				Rule rule = rules.get(i);
				Rule otherRule = other.rules.get(i);
				
				if (!rule.getRuleName().equals(otherRule.getRuleName())) {
					return false;
				}
				if (rule.getParams().size() != otherRule.getParams().size()) {
					return false;
				}
				for (int j = 0 ; j < rule.getParams().size(); j++) {
					if (!rule.getParams().get(j).equals(otherRule.getParams().get(j))) {
						return false;
					}
				}
			}
			return true;
    	}
    	
        return false;
    }

}
