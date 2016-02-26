package com.zx.bigdata.mapreduce.mapper;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.csvreader.CsvWriter;
import com.zx.bigdata.bean.datadef.DataSchema;
import com.zx.bigdata.bean.datadef.ReportTypeEnum;
import com.zx.bigdata.bean.datadef.SourceFileTypeEnum;
import com.zx.bigdata.bean.processdef.DataProcess;
import com.zx.bigdata.mapreduce.bean.MRCounterMap;
import com.zx.bigdata.mapreduce.bean.MRDataProcess;
import com.zx.bigdata.mapreduce.bean.MRDataSchema;
import com.zx.bigdata.mapreduce.bean.StatusCounterEnum;
import com.zx.bigdata.mapreduce.bean.ZXDBObjectKey;
import com.zx.bigdata.utils.MRUtils;
import com.zx.bigdata.utils.ZXBigDataException;

public class MRMapper extends Mapper<LongWritable, Text, ZXDBObjectKey, Writable> {
	private static final Logger LOG = Logger.getLogger(MRMapper.class);
	
	private ObjectMapper mapper = null;
	private SourceFileTypeEnum fileType;
	
	private MRDataSchema mrDataSchema = null;
	private MRDataProcess mrDataProcess = null;
	private MRCounterMap counterMap = null;
	private MultipleOutputs<ZXDBObjectKey, Writable> mos = null;

	protected void setup(Context context) throws IOException{
		// 获取数据模型
		String dataSchemaJson = context.getConfiguration().get("org.zx.bigdata.dataschema");
		// 获取数据流程
		String dataProcessJson = context.getConfiguration().get("org.zx.bigdata.dataprocess");
		
		this.counterMap = new MRCounterMap(context);
		mapper = new ObjectMapper();
		try {
			DataSchema dataSchema = mapper.readValue(dataSchemaJson, DataSchema.class);
			mrDataSchema = new MRDataSchema(dataSchema);
			
			DataProcess dataProcess = mapper.readValue(dataProcessJson, DataProcess.class);
			mrDataProcess = new MRDataProcess(dataProcess, mrDataSchema.getDataSchema());
			
			// 当报文的文件类型为文本文件的时候，获取报文头
			fileType = dataSchema.getFileType();
			
			String headLine = null;
			if (fileType == SourceFileTypeEnum.Plain) {
				Path path = ((FileSplit)context.getInputSplit()).getPath();
				BufferedReader in = new BufferedReader(new InputStreamReader(path.getFileSystem(context.getConfiguration()).open(path),MRUtils.ENCODING_UTF8),8124);
				headLine = in.readLine();
				in.close();				
			}
			mrDataSchema.calHeadValues(headLine);
			
			mos = new MultipleOutputs<ZXDBObjectKey, Writable>(context);
			LOG.info("init succesfully");

		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}
		
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();

		if (line==null || line.trim().isEmpty()) {//忽略空行
			return;
		}
		
		// 如果是text文件，跳过第一行的报文头
		if (key.get() == 0 && fileType == SourceFileTypeEnum.Plain) {
			return;
		}
		
		this.counterMap.get(StatusCounterEnum.NUM_INPUT_RECORD).increment(1);

		/** step 1:获取字段的值并进行校验。
		* 即：1. 根据信息段的字段定义，获取所有字段的值；<p>
		* 2.并根据字段的校验规则，对该值进行校验;<p>
		* 3.对信息段出现次数进行校验
		*/
		boolean isExpected = mrDataSchema.validDataSchema(line);
		
		if (isExpected) { // 合法数据
			context.getCounter(StatusCounterEnum.NUM_VALID_RECORD).increment(1);
			
			//计算二级索引
			List<ZXDBObjectKey> sndKeys = this.mrDataProcess.calSNDKeyValues(this.mrDataSchema.getCurrentRecord());
			for (ZXDBObjectKey sndKey : sndKeys) {
				context.write(sndKey, NullWritable.get());
				this.counterMap.get(StatusCounterEnum.NUM_NORMAL_SECONDARY_KEY).increment(1);
			}
			
			// 计算rowkey
			Map<ZXDBObjectKey, Writable> rowkeys;
			try {
				rowkeys = this.mrDataProcess.calRowKeyValues(this.mrDataSchema.getCurrentRecord());
				Counter statusCounter = null;
				if (this.mrDataSchema.getReportType() == ReportTypeEnum.NORMAL) {
					statusCounter = this.counterMap.get(StatusCounterEnum.NUM_NORMAL_ROWKEY);
				} else if (this.mrDataSchema.getReportType() == ReportTypeEnum.DELETE) {
					statusCounter = this.counterMap.get(StatusCounterEnum.NUM_DELETE_ROWKEY);
				}			
				for (Map.Entry<ZXDBObjectKey, Writable> entry : rowkeys.entrySet()) {
					context.write(entry.getKey(), entry.getValue());
					statusCounter.increment(1);
				}
			} catch (ZXBigDataException e) {
				e.printStackTrace();
				throw new IOException(e.getMessage());
			}
		} else { // 非法数据
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			CsvWriter writer = new CsvWriter(os,MRUtils.FIELD_DELIMITER.charAt(0),Charset.forName("UTF-8"));
			writer.setForceQualifier(true);
			// write filename to feedback
			writer.write(((FileSplit)context.getInputSplit()).getPath().getName());
			// write current record to feedback
			writer.write(line);
			writer.close();
			mos.write("feedback", NullWritable.get(), os.toString("UTF-8"));
			this.counterMap.get(StatusCounterEnum.NUM_INVALID_RECORD).increment(1);
			os.close();
		}
	}
}
