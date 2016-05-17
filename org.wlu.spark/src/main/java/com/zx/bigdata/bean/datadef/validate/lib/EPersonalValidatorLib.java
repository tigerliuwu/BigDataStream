package com.zx.bigdata.bean.datadef.validate.lib;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zx.bigdata.bean.datadef.Segment;
import com.zx.bigdata.bean.datadef.rules.ZXValidatorPrototype;
import com.zx.bigdata.bean.datadef.validate.AppSysCodeEnum;
import com.zx.bigdata.bean.datadef.validate.inter.IZXValidator;
import com.zx.bigdata.bean.feedback.ZXValidatorFeedBack;
import com.zx.bigdata.mapreduce.bean.ReportRecord;

public class EPersonalValidatorLib implements IZXValidator {
	public static final AppSysCodeEnum APP_SYS_CODE = AppSysCodeEnum.APP_NONBANK_PERSONAL; // 非银行类个人报文

	enum ReportType {

		SUBSISTENCE_ALLOWANCES("M"), // 个人低保
		PERSONAL_TAXES("D"); // 个人税务

		private ReportType(String reportType) {
			this.name = reportType;
		}

		public static ReportType fromString(String reportType) {
			for (ReportType type : ReportType.values()) {
				if (reportType.equalsIgnoreCase(type.name)) {
					return type;
				}
			}
			return null;
		}

		public String name;
	}

	private Map<String, ZXValidatorPrototype> validators = null;

	public EPersonalValidatorLib() {
		validators = new HashMap<String, ZXValidatorPrototype>();
	}

	public ZXValidatorPrototype getValidator(String reportType) {
		ZXValidatorPrototype validator = this.validators.get(reportType);
		if (validator != null) {
			return validator;
		}

		switch (ReportType.fromString(reportType)) {
		case SUBSISTENCE_ALLOWANCES:
			validator = new ZXSubsistenceAllowanceValidator();
			break;
		case PERSONAL_TAXES:
			validator = new ZXTaxValidator();
			break;
		default:
			throw new RuntimeException("没有对应的报文类型：" + reportType);
		}
		this.validators.put(reportType, validator);
		return validator;

	}

	// 个人低保的后台校验
	class ZXSubsistenceAllowanceValidator extends ZXValidatorPrototype {
		public ZXValidatorFeedBack validateRecord(ReportRecord record, ZXValidatorFeedBack retVal) {
			if (record.getIndicatorOfFeedback().equals("0")) { // 正常报文

			} else if (record.getIndicatorOfFeedback().equals("1")) { // 删除报文

			}

			return null;

		}
	}

	// 个人税务的后台校验
	class ZXTaxValidator extends ZXValidatorPrototype {
		public ZXValidatorFeedBack validateRecord(ReportRecord record, ZXValidatorFeedBack retVal) {
			if (record.getIndicatorOfFeedback().equals("0")) { // 正常报文
				// (30) 如果“欠税总额”和“本期应缴未缴税款”不为空时，“欠税总额”>=“本期应缴未缴税款”（1105 0107）
				String strbackTaxTotal = record.get(Segment.CONST_BASIC_KEY).get(0).get("backTaxTotal"); // 欠税总额
				long backTaxTotal = 0;
				if (!strbackTaxTotal.trim().isEmpty()) {
					backTaxTotal = Long.parseLong(strbackTaxTotal.trim());
				}

				// 本期应缴未缴税款
				String strcurrentUnpaidTaxTotal = record.get(Segment.CONST_BASIC_KEY).get(0)
						.get("currentUnpaidTaxTotal");
				long currentUnpaidTaxTotal = 0;
				if (!strcurrentUnpaidTaxTotal.trim().isEmpty()) {
					currentUnpaidTaxTotal = Long.parseLong(strcurrentUnpaidTaxTotal.trim());
				}
				if (backTaxTotal < currentUnpaidTaxTotal) {
					retVal.add("backTaxTotal", "unknowncode");
					if (retVal.isFull()) {
						return retVal;
					}
				}

				// (31) 如果“欠税总额”和“分税种欠税总额”不为空时，“欠税总额”>=“分税种欠税总额”（1105 0109）
				if (!strbackTaxTotal.trim().isEmpty()) {
					long taxUnpaidTotal = 0; // 分税种欠税总额
					List<Map<String, String>> maps = record.get("B");
					if (maps != null) {
						for (Map<String, String> map : maps) {
							String taxUnpaid = map.get("taxUnpaidTotal");
							if (!taxUnpaid.trim().isEmpty()) {
								taxUnpaidTotal += Long.parseLong(taxUnpaid.trim());
							}
						}
					}
					if (backTaxTotal < taxUnpaidTotal) {
						retVal.add("backTaxTotal", "unknowncode");
						if (retVal.isFull()) {
							return retVal;
						}
					}

				}

			} else if (record.getIndicatorOfFeedback().equals("1")) { // 删除报文

			}

			return retVal;

		}
	}

}
