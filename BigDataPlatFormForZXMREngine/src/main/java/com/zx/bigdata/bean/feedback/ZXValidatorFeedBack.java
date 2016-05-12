package com.zx.bigdata.bean.feedback;

import java.util.ArrayList;
import java.util.List;

/**
 * 做为校验反馈结果
 * 
 * @author liuwu
 *
 */
public class ZXValidatorFeedBack {

	private static final int CONST_ERR_TIMES = 5;

	private List<FeedbackRecord> feedbackRecords = new ArrayList<FeedbackRecord>(CONST_ERR_TIMES); // 保存校验失败的错误代码

	public boolean hasErrorCode() {
		return feedbackRecords.isEmpty() == false;
	}

	public boolean add(String fieldName, String errCode) {
		if (isFull()) {
			return false;
		}
		this.feedbackRecords.add(new FeedbackRecord(fieldName, errCode));
		return true;
	}

	public boolean isFull() {
		return this.feedbackRecords.size() >= CONST_ERR_TIMES;
	}

	public boolean isEmpty() {
		return this.feedbackRecords.isEmpty();
	}

	public List<FeedbackRecord> getAll() {
		return this.feedbackRecords;
	}
}
