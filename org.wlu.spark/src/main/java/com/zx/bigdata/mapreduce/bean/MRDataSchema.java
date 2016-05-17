package com.zx.bigdata.mapreduce.bean;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.zx.bigdata.bean.datadef.DataItem;
import com.zx.bigdata.bean.datadef.DataItemTypeEnum;
import com.zx.bigdata.bean.datadef.DataSchema;
import com.zx.bigdata.bean.datadef.OccurrencyRateEnum;
import com.zx.bigdata.bean.datadef.ReportTypeEnum;
import com.zx.bigdata.bean.datadef.Segment;
import com.zx.bigdata.bean.datadef.SourceFileTypeEnum;
import com.zx.bigdata.bean.datadef.rules.RulesExecutor;
import com.zx.bigdata.bean.datadef.rules.ZXValidatorPrototype;
import com.zx.bigdata.bean.datadef.validate.AppSysCodeEnum;
import com.zx.bigdata.bean.datadef.validate.inter.IZXValidator;
import com.zx.bigdata.bean.datadef.validate.lib.ReportValidatorRepository;
import com.zx.bigdata.bean.feedback.ZXValidatorFeedBack;

/**
 * 该类实现对数据模型DataSchema的封装，并提供一些接口实现对DataSchema的操作 目前只支持文本的操作，不支持xml
 * 
 * @author liuwu
 *
 */
public class MRDataSchema {

	private DataSchema dataSchema = null;
	// 用于保存一条记录中各个字段对应的合法值
	private ReportRecord recordVal = null;

	private SourceFileTypeEnum fileType;

	public MRDataSchema(DataSchema dataSchema) {
		this.dataSchema = dataSchema;
		fileType = dataSchema.getFileType();
		// 对recordVal 进行初始化
		recordVal = new ReportRecord(this.dataSchema);

	}

	public DataSchema getDataSchema() {
		return this.dataSchema;
	}

	/**
	 * 根据报文头的字段信息，获取相应字段的值，并按照Map<DataItem.name,value>的格式保存报文头数据
	 * <p>
	 * 注：报文头不需要进行校验
	 * 
	 * @param header
	 */
	public void calHeadValues(String headerLine) {
		String curValue = "";
		if (fileType == SourceFileTypeEnum.Plain) {
			Map<String, String> keyVal = new HashMap<String, String>();
			for (DataItem item : dataSchema.getSegments().get(Segment.CONST_HEADER_KEY).getDataItems()) {
				curValue = getCurValue(item, headerLine);
				keyVal.put(item.getName(), curValue);
			}
			recordVal.get(Segment.CONST_HEADER_KEY).add(keyVal);
		}
	}

	private String getCurValue(DataItem field, String line) {
		String curVal = null;
		if (fileType == SourceFileTypeEnum.Plain) {
			int start = field.getStartPose();
			int end = field.getEndPose();
			if (end >= start) {
				curVal = line.substring(start - 1, end);
			} else {
				// do something here
			}

		} else {
			// to do for xml
		}

		if (field.getDataItemType() != DataItemTypeEnum.N) {
			curVal = StringUtils.stripEnd(curVal, null);
		}

		return curVal;
	}

	/**
	 * 1. 校验除了报文头的信息段是否合法 2. 读取合法数据并保存 3. 对信息段出现次数进行校验
	 * 
	 * @param dataSchema
	 * @param line：报文记录（非报文头）
	 * @return
	 * @throws Exception
	 */
	public boolean validDataSchema(String line, ZXValidatorFeedBack feedback) throws IOException {

		int len = line.length();
		String curValue = null; // 保存当前数据列的临时值
		int position = 0; // 用于记录信息段在当前记录中的结束位置
		Map<String, String> segMap = null; // 用于保存一个segment字段的值

		// 清理recordVal
		clearRecordVal();

		// ****************获取基本信息段 开始*****************************
		Segment basicSeg = dataSchema.getSegments().get(Segment.CONST_BASIC_KEY);
		if (basicSeg == null) { // 如果基本信息段为空，数据模型为非法数据模型。
			throw new IOException("数据模型:\"" + dataSchema.getName() + "\"缺失基本信息段！");
		}

		segMap = new HashMap<String, String>();

		for (int i = 0; !feedback.isFull() && i < basicSeg.getDataItems().size(); i++) {
			DataItem item = basicSeg.getDataItems().get(i);
			curValue = getCurValue(item, line); // 获取字段对应的值
			/*
			 * if (this.dataSchema.getReportType() == ReportTypeEnum.NORMAL && i
			 * == 0) { // 第一个字段为记录长度 len = Integer.parseInt(curValue); if (len
			 * != line.length()) { isValid = false; break; } }
			 */
			RulesExecutor.executeRule(item, curValue, feedback);
			if (feedback.isEmpty()) {
				segMap.put(item.getName(), curValue);
			}

		}

		// ****************获取基本信息段 结束*****************************

		// ****************获取其它信息段 开始*****************************
		if (!feedback.isFull()) {
			// 获取基本信息段最后一个字段的结束位置
			if (fileType == SourceFileTypeEnum.Plain) {
				position = position + basicSeg.getDataItems().get(basicSeg.getDataItems().size() - 1).getEndPose();
			} else {
				// to do: 计算xml文件的基本信息段在文件中的结束位置
			}

			recordVal.get(basicSeg.getName()).add(segMap); // 保存基本信息段中的字段值

			while (!feedback.isFull() && position < len) {

				String subLine = line.substring(position); // 报文记录剩下的信息段

				String segKey = subLine.substring(0, 1);
				Segment seg = dataSchema.getSegments().get(segKey);

				if (seg != null) {

					segMap = new HashMap<String, String>();

					int i = -1;
					for (i = 0; !feedback.isFull() && i < seg.getDataItems().size(); i++) {
						DataItem item = seg.getDataItems().get(i);
						curValue = getCurValue(item, subLine); // 获取字段对应的值
						RulesExecutor.executeRule(item, curValue, feedback);
						if (feedback.isEmpty()) {
							segMap.put(item.getName(), curValue);
						}
					}

					// 获取基本信息段最后一个字段的结束位置
					if (fileType == SourceFileTypeEnum.Plain) {
						position = position + seg.getDataItems().get(i - 1).getEndPose();
					} else {
						// to do: 计算xml文件的基本信息段在文件中的结束位置
					}
					if (feedback.isEmpty()) {
						recordVal.get(segKey).add(segMap); // 保存基本信息段中的字段值
					}

				} else {
					throw new IOException("数据模型:\"" + dataSchema.getName() + "\"缺失信息段:\"" + segKey + "\"");
				}
			}
		}
		// ****************获取其它信息段 结束*****************************

		// 校验信息段出现的频次
		if (!feedback.isFull()) {
			validateSegOccurrency(feedback);
		}

		// ＊＊＊＊＊＊＊＊＊＊＊业务逻辑校验--开始＊＊＊＊＊＊＊＊＊＊＊

		// 非银行类个人报文
		IZXValidator validatorFactory = ReportValidatorRepository
				.get(AppSysCodeEnum.APP_NONBANK_PERSONAL.getAppSysCode());
		ZXValidatorPrototype validator = validatorFactory.getValidator(this.recordVal.getReportInfoType());
		if (!feedback.isFull()) {
			validator.validateRecord(this.recordVal, feedback);
		}
		// 非银行类企业报文
		// TODO

		// ＊＊＊＊＊＊＊＊＊＊＊业务逻辑校验--结束＊＊＊＊＊＊＊＊＊＊＊

		return feedback.isEmpty();

	}

	public ReportRecord getCurrentRecord() {
		return this.recordVal;
	}

	/**
	 * 清理recordVal中除Head segment对应的值意外所有的信息段中的值
	 */
	private void clearRecordVal() {
		for (Map.Entry<String, List<Map<String, String>>> entry : recordVal.entrySet()) {
			// 保留报文头中的数据
			if (Segment.CONST_HEADER_KEY.equals(entry.getKey())) {
				continue;
			}
			entry.getValue().clear();
		}
	}

	/**
	 * 校验各个信息段出现频次
	 */
	private boolean validateSegOccurrency(ZXValidatorFeedBack feedback) {
		boolean valid = true;
		for (Map.Entry<String, List<Map<String, String>>> entry : this.recordVal.entrySet()) {
			String segKey = entry.getKey();
			Segment seg = this.dataSchema.getSegments().get(segKey);
			if (seg == null) {
				valid = false;
				break;
			}

			OccurrencyRateEnum occurrencyRate = seg.getOccurrencyRate();
			switch (occurrencyRate) {
			case M_ONCE:
				if (entry.getValue().size() != 1) {
					valid = false;
				}
				break;

			case M_MULTIPLE:
				if (entry.getValue().size() < 1) {
					valid = false;
				}
				break;

			case O_ONCE:
				if (entry.getValue().size() > 1) {
					valid = false;
				}
				break;

			case O_MULTIPLE:
				break;

			default:
				throw new RuntimeException("信息段：\"+segKey+\"" + "没有对应的" + occurrencyRate.name());
			}

			if (!valid) {
				feedback.add(segKey, "unknowncode");
				if (feedback.isFull()) {
					break;
				}
			}
		}
		if (!valid) {
		}

		return valid;
	}

	public ReportTypeEnum getReportType() {
		return this.dataSchema.getReportType();
	}

}
