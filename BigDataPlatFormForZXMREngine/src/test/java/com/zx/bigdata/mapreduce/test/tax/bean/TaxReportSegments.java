package com.zx.bigdata.mapreduce.test.tax.bean;

import java.util.HashMap;
import java.util.Map;

import com.zx.bigdata.bean.datadef.DataItem;
import com.zx.bigdata.bean.datadef.DataItemTypeEnum;
import com.zx.bigdata.bean.datadef.OccurrencyRateEnum;
import com.zx.bigdata.bean.datadef.Segment;
import com.zx.bigdata.bean.datadef.rules.Rule;
import com.zx.bigdata.bean.datadef.rules.RuleMethodEnum;

public class TaxReportSegments {

	private static Map<String, Segment> segments;

	static {
		init();
	}

	public TaxReportSegments() {
		// init();
	}

	private static void init() {
		segments = new HashMap<String, Segment>();
		segments.put(Segment.CONST_HEADER_KEY, initHeaderSegment());
		segments.put(Segment.CONST_BASIC_KEY, initBasicSegment());
	}

	public static Segment getSegment(String segName) {

		return segments.get(segName);
	}

	public static Map<String, Segment> getSegments() {
		return segments;
	}

	private static Segment initHeaderSegment() {
		Segment segment = new Segment();
		segment.setName(Segment.CONST_HEADER_KEY);
		segment.setOccurrencyRate(OccurrencyRateEnum.M_ONCE);

		// *** head items** start **
		// 数据格式版本
		DataItem item = new DataItem();
		item.setName("version");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(1);
		item.setEndPose(3);
		item.getRules().add(new Rule(RuleMethodEnum.IS_VERSION_FORMAT.getMethodName()));
		segment.getDataItems().add(item);
		// 报送机构代码
		item = new DataItem();
		item.setName("COURTCODE");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(4);
		item.setEndPose(17);
		segment.getDataItems().add(item);
		// 报文生成时间YYYYMMDDHHMMSSS
		item = new DataItem();
		item.setName("REGISTERDATE");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(18);
		item.setEndPose(31);
		Rule rul = new Rule();
		rul.setRuleName(RuleMethodEnum.CHECK_DATE.getMethodName());
		rul.getParams().add("YYYYMMDDHHMMSS");
		item.getRules().add(rul);
		segment.getDataItems().add(item);
		// 报文类别
		item = new DataItem();
		item.setName("LITIGATIONTYPE");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(32);
		item.setEndPose(32);
		segment.getDataItems().add(item);
		// 应用系统代码
		item = new DataItem();
		item.setName("ORGINALCASENO");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(33);
		item.setEndPose(33);
		segment.getDataItems().add(item);
		// 报文类型
		item = new DataItem();
		item.setName("ORGINALCASETYPE");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(34);
		item.setEndPose(34);
		segment.getDataItems().add(item);
		// 反馈标志
		item = new DataItem();
		item.setName("FEEDBACKSIGN");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(35);
		item.setEndPose(35);
		segment.getDataItems().add(item);
		// 国税／地税标识
		item = new DataItem();
		item.setName("TAXSYMBOL");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(36);
		item.setEndPose(37);
		segment.getDataItems().add(item);
		// 联系人
		item = new DataItem();
		item.setName("CONTACKOR");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(38);
		item.setEndPose(67);
		segment.getDataItems().add(item);
		// 联系电话
		item = new DataItem();
		item.setName("CONTACKNO");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(68);
		item.setEndPose(92);
		segment.getDataItems().add(item);
		// 预留字段
		item = new DataItem();
		item.setName("OBLIGATE");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(93);
		item.setEndPose(122);
		segment.getDataItems().add(item);
		// *** head items ** end ***

		return segment;

	}

	private static Segment initBasicSegment() {
		Segment segment = null;

		segment = new Segment();
		segment.setName(Segment.CONST_BASIC_KEY);
		segment.setOccurrencyRate(OccurrencyRateEnum.M_ONCE);

		// 账户记录长度
		DataItem item = new DataItem();
		item.setName("accountRLen");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(1);
		item.setEndPose(8);
		segment.getDataItems().add(item);

		// 信息类别
		item = new DataItem();
		item.setName("reportInformationType");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(9);
		item.setEndPose(9);
		segment.getDataItems().add(item);

		// 主管税务机关名称
		item = new DataItem();
		item.setName("taxOrgName");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(10);
		item.setEndPose(59);
		segment.getDataItems().add(item);

		// 主管税务机关代码
		item = new DataItem();
		item.setName("taxOrgCode");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(60);
		item.setEndPose(73);
		segment.getDataItems().add(item);

		// 税务报送统计时间
		item = new DataItem();
		item.setName("taxReportTime");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(74);
		item.setEndPose(81);
		segment.getDataItems().add(item);

		// 纳税人中文名称
		item = new DataItem();
		item.setName("taxPayerName");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(82);
		item.setEndPose(161);
		segment.getDataItems().add(item);

		// 经营范围
		item = new DataItem();
		item.setName("businessScope");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(162);
		item.setEndPose(261);
		segment.getDataItems().add(item);

		// 纳税人识别号
		item = new DataItem();
		item.setName("taxPayerId");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(262);
		item.setEndPose(281);
		segment.getDataItems().add(item);

		// 姓名
		item = new DataItem();
		item.setName("NAME");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(282);
		item.setEndPose(311);
		segment.getDataItems().add(item);

		// 证件类型
		item = new DataItem();
		item.setName("CERTTYPE");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(312);
		item.setEndPose(312);
		segment.getDataItems().add(item);

		// 证件号码
		item = new DataItem();
		item.setName("CERTNO");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(313);
		item.setEndPose(330);
		segment.getDataItems().add(item);

		// 纳税人注册类型
		item = new DataItem();
		item.setName("taxPayerRType");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(331);
		item.setEndPose(334);
		segment.getDataItems().add(item);

		// 登记状态
		item = new DataItem();
		item.setName("regStatus");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(335);
		item.setEndPose(335);
		segment.getDataItems().add(item);

		// 行业分类
		item = new DataItem();
		item.setName("tradeClass");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(336);
		item.setEndPose(336);
		segment.getDataItems().add(item);

		// 纳税人地址
		item = new DataItem();
		item.setName("taxPayerAddr");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(337);
		item.setEndPose(416);
		segment.getDataItems().add(item);

		// 纳税人联系电话
		item = new DataItem();
		item.setName("taxPayerPhoneNo");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(417);
		item.setEndPose(441);
		segment.getDataItems().add(item);

		// 登记证有效期止
		item = new DataItem();
		item.setName("certExpireDate");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(442);
		item.setEndPose(449);
		segment.getDataItems().add(item);

		// 纳税人阻止出境标识
		item = new DataItem();
		item.setName("taxPayerBlockStatus");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(450);
		item.setEndPose(450);
		segment.getDataItems().add(item);

		// 注册资金金额
		item = new DataItem();
		item.setName("regCapital");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(451);
		item.setEndPose(465);
		segment.getDataItems().add(item);

		// 纳税状态
		item = new DataItem();
		item.setName("taxStatus");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(466);
		item.setEndPose(466);
		segment.getDataItems().add(item);

		// 纳税状态
		item = new DataItem();
		item.setName("taxStatus");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(466);
		item.setEndPose(466);
		segment.getDataItems().add(item);

		// 欠税总额
		item = new DataItem();
		item.setName("backTaxTotal");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(467);
		item.setEndPose(481);
		segment.getDataItems().add(item);

		// 本期应缴未缴税款
		item = new DataItem();
		item.setName("currentUnpaidTaxTotal");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(482);
		item.setEndPose(496);
		segment.getDataItems().add(item);

		// 发生地点
		item = new DataItem();
		item.setName("scene");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(497);
		item.setEndPose(502);
		segment.getDataItems().add(item);

		// 保留字段
		item = new DataItem();
		item.setName("obligate");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(503);
		item.setEndPose(532);
		item.getRules().add(new Rule(RuleMethodEnum.IS_NULLABLE.getMethodName()));
		segment.getDataItems().add(item);

		return segment;
	}

	public static void initDetailedTaxSegment() {
		Segment segment = segments.get("B");
		if (segment != null) {
			return;
		}

		segment = new Segment();
		segments.put("B", segment);
		segment.setName("B");
		segment.setOccurrencyRate(OccurrencyRateEnum.O_MULTIPLE);

		// 信息类别
		DataItem item = new DataItem();
		item.setName("reportInformationType");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(1);
		item.setEndPose(1);
		segment.getDataItems().add(item);

		// 税种
		item = new DataItem();
		item.setName("taxType");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(2);
		item.setEndPose(6);
		segment.getDataItems().add(item);

		// 分税种纳税状态
		item = new DataItem();
		item.setName("taxPaidStatus");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(7);
		item.setEndPose(7);
		segment.getDataItems().add(item);

		// 分税种欠税总额
		item = new DataItem();
		item.setName("taxUnpaidTotal");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(8);
		item.setEndPose(22);
		segment.getDataItems().add(item);

		// 分税种本期应缴未缴税款
		item = new DataItem();
		item.setName("taxCurUnpaidTotal");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(23);
		item.setEndPose(37);
		segment.getDataItems().add(item);

		// 保留字段
		item = new DataItem();
		item.setName("obligate");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(38);
		item.setEndPose(67);
		item.getRules().add(new Rule(RuleMethodEnum.IS_NULLABLE.getMethodName()));
		segment.getDataItems().add(item);

		return;
	}

}
