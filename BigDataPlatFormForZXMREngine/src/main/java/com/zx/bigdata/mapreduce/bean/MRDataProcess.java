package com.zx.bigdata.mapreduce.bean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.zx.bigdata.bean.datadef.DataSchema;
import com.zx.bigdata.bean.datadef.OccurrencyRateEnum;
import com.zx.bigdata.bean.datadef.ReportTypeEnum;
import com.zx.bigdata.bean.processdef.ColumnObject;
import com.zx.bigdata.bean.processdef.DBSchema;
import com.zx.bigdata.bean.processdef.DataProcess;
import com.zx.bigdata.bean.processdef.RowKeyObject;
import com.zx.bigdata.bean.processdef.SecondaryIndex;
import com.zx.bigdata.bean.processdef.SegmentDataItemPair;
import com.zx.bigdata.utils.MRUtils;
import com.zx.bigdata.utils.ZXBigDataException;

/**
 * 对DataProcess进行封装，并提供接口对DataProcess进行操作
 * 
 * @author liuwu
 *
 */
public class MRDataProcess {

	private DataProcess dataProcess = null;
	private DataSchema dataSchema = null;

	private ObjectMapper objMapper = null;

	public MRDataProcess(DataProcess process, DataSchema dataSchema) {
		this.dataProcess = process;
		this.dataSchema = dataSchema;
		this.objMapper = new ObjectMapper();
	}

	/**
	 * 根据传递的rowkeyObject和一条报文记录，组装一个hbase里面的rowkey
	 * 
	 * @param record
	 * @param key
	 * @return
	 */
	public StringBuilder calRowKey(ReportRecord record, RowKeyObject key) {
		// 跟该二级索引关联的rowkey
		StringBuilder rowKeyBuilder = new StringBuilder();
		RowKeyObject rowKey = key;
		if (rowKey != null) {
			// 唯一标识
			for (SegmentDataItemPair pair : rowKey.getIdCode()) {
				if (rowKeyBuilder.length() > 0) {
					rowKeyBuilder.append(MRUtils.KEY_SPLIT_DEL);
				}
				rowKeyBuilder.append(record.getDataItemValueFromSeg(pair.getSegmentName(), 0, pair.getDataItemName()));
			}

			// 数据源区分段
			if (rowKeyBuilder.length() > 0) {
				rowKeyBuilder.append(MRUtils.KEY_SPLIT_DEL);
			}
			SegmentDataItemPair reportType = rowKey.getReportInformationType();
			rowKeyBuilder.append(
					record.getDataItemValueFromSeg(reportType.getSegmentName(), 0, reportType.getDataItemName()));

			// 删除报文
			if (this.dataSchema.getReportType() == ReportTypeEnum.DELETE) {
				return rowKeyBuilder;
			}

			// 业务类型区分段
			for (SegmentDataItemPair pair : rowKey.getBusiKeys()) {
				if (rowKeyBuilder.length() > 0) {
					rowKeyBuilder.append(MRUtils.KEY_SPLIT_DEL);
				}
				rowKeyBuilder.append(record.getDataItemValueFromSeg(pair.getSegmentName(), 0, pair.getDataItemName()));
			}

			// 时间戳
			if (rowKeyBuilder.length() > 0) {
				rowKeyBuilder.append(MRUtils.KEY_SPLIT_DEL);
			}
			SegmentDataItemPair time = rowKey.getTime();
			rowKeyBuilder.append(record.getDataItemValueFromSeg(time.getSegmentName(), 0, time.getDataItemName()));
		}

		return rowKeyBuilder;
	}

	/**
	 * 根据ColumnObject计算该列存在hbase表中的value，返回值是一个json
	 * <p>
	 * 
	 * @param colObj
	 * @return
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonGenerationException
	 */
	private String calDBColumnObjectValue(ReportRecord record, ColumnObject colObj)
			throws ZXBigDataException, JsonGenerationException, JsonMappingException, IOException {

		String result = null;
		List<JSONValueBuilder> results = new ArrayList<JSONValueBuilder>();

		for (Map<String, String> map : record.get(colObj.getSegName())) {
			JSONValueBuilder builder = new JSONValueBuilder();
			for (SegmentDataItemPair dataItem : colObj.getDataItems()) {
				builder.appendKey(dataItem.getDataItemName());
				if (colObj.getSegName().equals(dataItem.getSegmentName())) { // 来自于相同的信息段
					builder.appendValue(map.get(dataItem.getDataItemName()));
				} else { // 数据段来源于head 或者 basic 信息段
					builder.appendValue(
							record.getDataItemValueFromSeg(dataItem.getSegmentName(), 0, dataItem.getDataItemName()));
				}
			}
			results.add(builder);
		}

		OccurrencyRateEnum occurRate = this.dataSchema.getSegments().get(colObj.getSegName()).getOccurrencyRate();
		switch (occurRate) {
		case M_ONCE:
			if (results.size() != 1) {
				throw new ZXBigDataException(
						"信息段：\"" + colObj.getSegName() + "\"必须出现一次，而事实上出现了" + results.size() + "次");
			}
			result = results.get(0).toString();
			break;
		case O_ONCE:
			if (results.size() > 1) {
				throw new ZXBigDataException(
						"信息段：\"" + colObj.getSegName() + "\"最多出现一次，而事实上出现了" + results.size() + "次");
			}
			if (!results.isEmpty()) {
				result = results.get(0).toString();
			} else {
				result = null;
			}
			break;
		case M_MULTIPLE:
			if (results.size() < 1) {
				throw new ZXBigDataException(
						"信息段：\"" + colObj.getSegName() + "\"至少出现一次，而事实上出现了" + results.size() + "次");
			}
			result = objMapper.writeValueAsString(results);
		case O_MULTIPLE:
			if (results.isEmpty()) {
				result = null;
			} else {
				result = objMapper.writeValueAsString(results);
			}
			break;
		default:
			result = null;
			break;
		}

		return result;
	}

	/**
	 * 
	 * @param record
	 * @return
	 * @throws IOException
	 * @throws ZXBigDataException
	 * @throws JsonMappingException
	 * @throws JsonGenerationException
	 */
	public Map<ZXDBObjectKey, Writable> calRowKeyValues(ReportRecord record)
			throws JsonGenerationException, JsonMappingException, ZXBigDataException, IOException {
		Map<ZXDBObjectKey, Writable> results = new HashMap<ZXDBObjectKey, Writable>();

		for (DBSchema schema : this.dataProcess.getDbSchemas()) {
			String strkey = this.calRowKey(record, schema.getRowKeyObject()).toString();
			ZXDBObjectKey key = new ZXDBObjectKey(strkey);
			MapWritable value = new MapWritable();
			for (ColumnObject colObj : schema.getColumnObjects()) {
				String jsonValue = this.calDBColumnObjectValue(record, colObj);
				if (jsonValue != null && !jsonValue.isEmpty()) {
					value.put(new Text(colObj.getSegName()), new Text(jsonValue));
				}
			}
			if (value.size() == 0) {
				results.put(key, NullWritable.get());
			} else {
				results.put(key, value);
			}
		}

		return results;
	}

	/**
	 * 二级索引所需的列必须来源于报文头和基本信息段，根据这两个信息段所选的字段组装成一个rowkey
	 * 
	 * @param record
	 * @return
	 */
	public List<ZXDBObjectKey> calSNDKeyValues(ReportRecord record) {
		List<ZXDBObjectKey> results = new ArrayList<ZXDBObjectKey>();

		if (this.dataProcess.getReportType() == ReportTypeEnum.DELETE) {
			return results;
		}

		for (SecondaryIndex skey : this.dataProcess.getsKeys()) {
			StringBuilder builder = new StringBuilder();
			for (SegmentDataItemPair pair : skey.getKey()) {
				if (builder.length() > 0) {
					builder.append(MRUtils.KEY_SPLIT_DEL);
				}
				builder.append(record.getDataItemValueFromSeg(pair.getSegmentName(), 0, pair.getDataItemName()));
			}

			// 跟该二级索引关联的rowkey
			StringBuilder rowKeyBuilder = calRowKey(record, skey.getRowKeyObject());

			if (skey.isDeleteSKey()) {
				if (builder.length() > 0) {
					builder.append(MRUtils.KEY_SPLIT_DEL);
				}

				// 数据源区分段
				SegmentDataItemPair reportType = skey.getRowKeyObject().getReportInformationType();
				builder.append(
						record.getDataItemValueFromSeg(reportType.getSegmentName(), 0, reportType.getDataItemName()));

				// 增加常量del作为rowkey的一部分
				builder.append(MRUtils.KEY_SPLIT_DEL);
				builder.append(MRUtils.CONSTANT_DELETE);

				// 增加跟该二级删除索引挂钩的rowkey
				builder.append(MRUtils.KEY_SPLIT_DEL);
				builder.append(rowKeyBuilder);

				ZXDBObjectKey keyValue = new ZXDBObjectKey(builder.toString(), true);
				results.add(keyValue);
			} else if (rowKeyBuilder.length() > 0) {
				// 存在跟二级索引挂钩的rowkey(即正常的二级索引)，进行双向存贮

				ZXDBObjectKey keyValue = new ZXDBObjectKey(
						builder.toString() + MRUtils.KEY_SPLIT_DEL + rowKeyBuilder.toString(), true);
				results.add(keyValue);

				keyValue = new ZXDBObjectKey(rowKeyBuilder.toString() + MRUtils.KEY_SPLIT_DEL + builder.toString(),
						true);
				results.add(keyValue);
			} else {
				// 不存在跟二级索引挂钩的rowkey
				ZXDBObjectKey keyValue = new ZXDBObjectKey(builder.toString(), true);
				results.add(keyValue);
			}

		}

		return results;
	}

}
