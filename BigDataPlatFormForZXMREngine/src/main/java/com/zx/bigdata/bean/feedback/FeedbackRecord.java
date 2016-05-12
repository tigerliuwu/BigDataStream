package com.zx.bigdata.bean.feedback;

/**
 * 代表反馈报文中一条记录
 * 
 * @author liuwu
 *
 */
public class FeedbackRecord {
	protected String errCode; // 错误代码
	protected String errField; // 出错字段标识符

	public FeedbackRecord(String field, String errCode) {
		this.errField = field;
		this.errCode = errCode;
	}

	public String getErrCode() {
		return errCode;
	}

	public void setErrCode(String errCode) {
		this.errCode = errCode;
	}

	public String getErrField() {
		return errField;
	}

	public void setErrField(String errField) {
		this.errField = errField;
	}

}
