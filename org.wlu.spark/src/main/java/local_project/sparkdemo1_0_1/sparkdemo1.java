package local_project.sparkdemo1_0_1;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.streaming.ui.BatchUIData;

import routines.TalendString;
import routines.system.SparkStreamingRunStat;
import routines.system.SparkStreamingRunStat.StatBean;
import routines.system.api.TalendJob;
import scala.Option;

@SuppressWarnings("unused")
/**
 * Job: sparkdemo1 Purpose: <br>
 * Description: <br>
 * 
 * @author user@talend.com
 * @version 6.2.0.20160510_1929-EP
 * @status
 */
public class sparkdemo1 implements TalendJob {
	static {
		System.setProperty("TalendJob.log", "sparkdemo1.log");
	}

	private final static String utf8Charset = "UTF-8";
	private GlobalVar globalMap = null;

	private static class GlobalVar {
		public static final String GLOBALVAR_PARAMS_PREFIX = "talend.globalvar.params.";
		private Configuration job;
		private java.util.Map<String, Object> map;

		public GlobalVar(Configuration job) {
			this.job = job;
			this.map = new java.util.HashMap<String, Object>();
		}

		public Object get(String key) {
			String tempValue = job.get(GLOBALVAR_PARAMS_PREFIX + key);
			if (tempValue != null) {
				return SerializationUtils.deserialize(Base64.decodeBase64(StringUtils.getBytesUtf8(tempValue)));
			} else {
				return null;
			}
		}

		public void put(String key, Object value) {
			if (value == null)
				return;
			job.set(GLOBALVAR_PARAMS_PREFIX + key,
					StringUtils.newStringUtf8(Base64.encodeBase64(SerializationUtils.serialize((Serializable) value))));
		}

		public void putLocal(String key, Object value) {
			map.put(key, value);
		}

		public Object getLocal(String key) {
			return map.get(key);
		}
	}

	// create and load default properties
	private java.util.Properties defaultProps = new java.util.Properties();

	public static class ContextProperties extends java.util.Properties {

		private static final long serialVersionUID = 1L;

		public static final String CONTEXT_FILE_NAME = "talend.context.fileName";
		public static final String CONTEXT_KEYS = "talend.context.keys";
		public static final String CONTEXT_PARAMS_PREFIX = "talend.context.params.";
		public static final String CONTEXT_PARENT_KEYS = "talend.context.parent.keys";
		public static final String CONTEXT_PARENT_PARAMS_PREFIX = "talend.context.parent.params.";

		public ContextProperties(java.util.Properties properties) {
			super(properties);
		}

		public ContextProperties() {
			super();
		}

		public ContextProperties(Configuration job) {
			super();
			String contextFileName = (String) job.get(CONTEXT_FILE_NAME);
			try {
				if (contextFileName != null && !"".equals(contextFileName)) {
					java.io.File contextFile = new java.io.File(contextFileName);
					if (contextFile.exists()) {
						java.io.InputStream contextIn = contextFile.toURI().toURL().openStream();
						this.load(contextIn);
						contextIn.close();
					} else {
						java.io.InputStream contextIn = sparkdemo1.class.getClassLoader()
								.getResourceAsStream("local_project/sparkdemo1_0_1/contexts/" + contextFileName);
						if (contextIn != null) {
							this.load(contextIn);
							contextIn.close();
						}
					}
				}
				Object contextKeys = job.get(CONTEXT_KEYS);
				if (contextKeys != null) {
					java.util.StringTokenizer st = new java.util.StringTokenizer(contextKeys.toString(), " ");
					while (st.hasMoreTokens()) {
						String contextKey = st.nextToken();
						if ((String) job.get(CONTEXT_PARAMS_PREFIX + contextKey) != null) {
							this.put(contextKey, job.get(CONTEXT_PARAMS_PREFIX + contextKey));
						}
					}
				}
				Object contextParentKeys = job.get(CONTEXT_PARENT_KEYS);
				if (contextParentKeys != null) {
					java.util.StringTokenizer st = new java.util.StringTokenizer(contextParentKeys.toString(), " ");
					while (st.hasMoreTokens()) {
						String contextKey = st.nextToken();
						if ((String) job.get(CONTEXT_PARENT_PARAMS_PREFIX + contextKey) != null) {
							this.put(contextKey, job.get(CONTEXT_PARENT_PARAMS_PREFIX + contextKey));
						}
					}
				}

				this.loadValue(null, job);
			} catch (java.io.IOException ie) {
				System.err.println("Could not load context " + contextFileName);
				ie.printStackTrace();
			}
		}

		public void synchronizeContext() {
		}

		public void loadValue(java.util.Properties context_param, Configuration job) {
		}
	}

	private ContextProperties context = new ContextProperties();

	public ContextProperties getContext() {
		return this.context;
	}

	public static class TalendSparkStreamingListener
			extends org.apache.spark.streaming.ui.StreamingJobProgressListener {

		private int batchCompleted = 0;
		private int batchStarted = 0;
		private String lastProcessingDelay = "";
		private String lastSchedulingDelay = "";
		private String lastTotalDelay = "";

		public TalendSparkStreamingListener(org.apache.spark.streaming.StreamingContext ssc) {
			super(ssc);
			StatBean sb = runStat.createSparkStreamingStatBean();
			sb.setSubjobId("1");
			sb.setBatchCompleted(this.batchCompleted);
			sb.setBatchStarted(this.batchStarted);
			sb.setLastProcessingDelay(this.lastProcessingDelay);
			sb.setLastSchedulingDelay(this.lastSchedulingDelay);
			sb.setLastTotalDelay(this.lastTotalDelay);
			runStat.updateSparkStreamingData(sb);
		}

		@Override
		public void onBatchStarted(org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted batchStarted) {
			super.onBatchStarted(batchStarted);
			this.batchStarted = this.batchStarted + 1;
			StatBean sb = runStat.createSparkStreamingStatBean();
			sb.setSubjobId("1");
			sb.setBatchCompleted(this.batchCompleted);
			sb.setBatchStarted(this.batchStarted);
			sb.setLastProcessingDelay(this.lastProcessingDelay);
			sb.setLastSchedulingDelay(this.lastSchedulingDelay);
			sb.setLastTotalDelay(this.lastTotalDelay);
			runStat.updateSparkStreamingData(sb);
		}

		public void onBatchCompleted(
				org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted batchCompleted) {
			super.onBatchCompleted(batchCompleted);
			Option<BatchUIData> lastCompletedBatch = this.lastCompletedBatch();
			synchronized (lastCompletedBatch) {
				this.batchCompleted = this.batchCompleted + 1;
				if (!lastCompletedBatch.isEmpty())
					this.lastProcessingDelay = lastCompletedBatch.get().processingDelay().get() + "";
				if (!lastCompletedBatch.isEmpty())
					this.lastSchedulingDelay = lastCompletedBatch.get().schedulingDelay().get() + "";
				if (!lastCompletedBatch.isEmpty())
					this.lastTotalDelay = lastCompletedBatch.get().totalDelay().get() + "";
				StatBean sb = runStat.createSparkStreamingStatBean();
				sb.setSubjobId("1");
				sb.setBatchCompleted(this.batchCompleted);
				sb.setBatchStarted(this.batchStarted);
				sb.setLastProcessingDelay(this.lastProcessingDelay);
				sb.setLastSchedulingDelay(this.lastSchedulingDelay);
				sb.setLastTotalDelay(this.lastTotalDelay);
				runStat.updateSparkStreamingData(sb);
			}
		}
	}

	private static SparkStreamingRunStat runStat = new SparkStreamingRunStat();

	private final static String jobVersion = "0.1";
	private final static String jobName = "sparkdemo1";
	private final static String projectName = "LOCAL_PROJECT";
	public Integer errorCode = null;

	private static String currentComponent = "";

	private final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
	private final java.io.PrintStream errorMessagePS = new java.io.PrintStream(new java.io.BufferedOutputStream(baos));

	public String getExceptionStackTrace() {
		if ("failure".equals(this.getStatus())) {
			errorMessagePS.flush();
			return baos.toString();
		}
		return null;
	}

	// should be remove later
	public void setDataSources(java.util.Map<String, javax.sql.DataSource> dataSources) {

	}

	public void tHDFSConfiguration_1PostProcess(final org.apache.spark.streaming.api.java.JavaStreamingContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
		final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {
			throw e;
		}
	}

	public void tHDFSConfiguration_1Process(final org.apache.spark.streaming.api.java.JavaStreamingContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
		final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {

			throw e;
		}
	}

	public void tHBaseConfiguration_1PostProcess(final org.apache.spark.streaming.api.java.JavaStreamingContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
		final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {
			throw e;
		}
	}

	public void tHBaseConfiguration_1Process(final org.apache.spark.streaming.api.java.JavaStreamingContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
		final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {

			throw e;
		}
	}

	public void tSparkConfiguration_1PostProcess(final org.apache.spark.streaming.api.java.JavaStreamingContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
		final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {
			throw e;
		}
	}

	public void tSparkConfiguration_1Process(final org.apache.spark.streaming.api.java.JavaStreamingContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
		final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {

			throw e;
		}
	}

	/**
	 * [tFileInputDelimited_1_HDFSInputFormat sparkstreamingcode ] start
	 */

	// read the input format line by line
	public static class row_tFileInputDelimited_1_HDFSInputFormatStructInputFormat extends
			org.talend.hadoop.mapred.lib.file.TDelimitedFileInputFormat<NullWritable, row_tFileInputDelimited_1_HDFSInputFormatStruct> {
		private ContextProperties context;

		// init
		public void setConf(Configuration conf) {
			context = new ContextProperties(conf);
			setInputPath("hdfs://ecX-XXX-XXX-XXX-XXX.compute-XXX.amazonaws.com:9000/" + "/user/liuwu/in");
		}

		public RecordReader<NullWritable, row_tFileInputDelimited_1_HDFSInputFormatStruct> getRecordReader(
				InputSplit split, JobConf job, Reporter reporter) throws IOException {
			if (reporter != null) {
				reporter.setStatus(split.toString());
			}
			return new HDFSRecordReader(job, (FileSplit) split, "\n".getBytes());
		}

		protected static class HDFSRecordReader extends
				org.talend.hadoop.mapred.lib.file.TDelimitedFileRecordReader<NullWritable, row_tFileInputDelimited_1_HDFSInputFormatStruct> {

			private ContextProperties context;

			protected HDFSRecordReader(JobConf job, FileSplit split, byte[] rowSeparator) throws IOException {
				super(job, split, rowSeparator);
				this.context = new ContextProperties(job);
			}

			public boolean next(NullWritable key, row_tFileInputDelimited_1_HDFSInputFormatStruct value)
					throws IOException {
				Text textValue = new Text();
				boolean hasNext = next(textValue);
				value.line = textValue.toString();

				return hasNext;
			}

			public NullWritable createKey() {
				return NullWritable.get();
			}

			public row_tFileInputDelimited_1_HDFSInputFormatStruct createValue() {
				return new row_tFileInputDelimited_1_HDFSInputFormatStruct();
			}
		}

	}

	/**
	 * [tFileInputDelimited_1_HDFSInputFormat sparkstreamingcode ] stop
	 */
	/**
	 * [tFileInputDelimited_1_ExtractDelimited sparkstreamingcode ] start
	 */

	public static class tFileInputDelimited_1_ExtractDelimited_Function implements
			org.apache.spark.api.java.function.PairFlatMapFunction<scala.Tuple2<NullWritable, row_tFileInputDelimited_1_HDFSInputFormatStruct>, NullWritable, row1Struct> {

		private ContextProperties context = null;

		public tFileInputDelimited_1_ExtractDelimited_Function(JobConf job) {
			this.context = new ContextProperties(job);
		}

		public java.lang.Iterable<scala.Tuple2<NullWritable, row1Struct>> call(
				scala.Tuple2<NullWritable, row_tFileInputDelimited_1_HDFSInputFormatStruct> data)
						throws java.lang.Exception {
			java.util.List<scala.Tuple2<NullWritable, row1Struct>> outputs = new java.util.ArrayList<scala.Tuple2<NullWritable, row1Struct>>();
			row1Struct row1 = new row1Struct();
			row_tFileInputDelimited_1_HDFSInputFormatStruct row_tFileInputDelimited_1_HDFSInputFormat = data._2;

			if (row_tFileInputDelimited_1_HDFSInputFormat.line == null)
				return null;

			try {
				String[] values = routines.system.StringUtils
						.splitNotRegex(row_tFileInputDelimited_1_HDFSInputFormat.line.toString(), ";");

				String temp = "";
				if (values.length > 0) {
					temp = values[0];
					row1.newColumn = temp;
				} else {
					row1.newColumn = null;
				}
				if (values.length > 1) {
					temp = values[1];
					row1.newColumn1 = temp;
				} else {
					row1.newColumn1 = null;
				}

				outputs.add(new scala.Tuple2<NullWritable, row1Struct>(NullWritable.get(), row1));
				row1 = new row1Struct();
			} catch (RuntimeException ex) {
				// Die immediately on errors.
				throw new IOException(ex);
			}
			return outputs;
		}
	}

	/**
	 * [tFileInputDelimited_1_ExtractDelimited sparkstreamingcode ] stop
	 */
	/**
	 * [tFilterColumns_1 sparkstreamingcode ] start
	 */

	public static class tFilterColumns_1_Function implements
			org.apache.spark.api.java.function.PairFunction<scala.Tuple2<NullWritable, row1Struct>, NullWritable, row2Struct> {

		private ContextProperties context = null;

		public tFilterColumns_1_Function(JobConf job) {
			this.context = new ContextProperties(job);
		}

		public scala.Tuple2<NullWritable, row2Struct> call(scala.Tuple2<NullWritable, row1Struct> data)
				throws java.lang.Exception {
			row2Struct row2 = new row2Struct();
			row1Struct row1 = data._2;

			row2.newColumn = row1.newColumn;

			return new scala.Tuple2<NullWritable, row2Struct>(NullWritable.get(), row2);
		}
	}

	/**
	 * [tFilterColumns_1 sparkstreamingcode ] stop
	 */
	/**
	 * [tHBaseOutput_1 sparkstreamingcode ] start
	 */

	public static class tHBaseOutput_1StructOutputFormat implements OutputFormat<NullWritable, row2Struct> {
		protected static class HBaseTableRecordWriter implements RecordWriter<NullWritable, row2Struct> {
			private org.apache.hadoop.hbase.client.HTable m_table;

			ContextProperties context;

			public HBaseTableRecordWriter(org.apache.hadoop.hbase.client.HTable table, Configuration job) {
				this.m_table = table;
				context = new ContextProperties(job);
			}

			private void writeObject(row2Struct value) throws IOException {
				byte[] rowKey = org.apache.hadoop.hbase.util.Bytes.toBytes(value.newColumn);
				org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(rowKey);
				byte[] temp = null;

				this.m_table.put(put);
			}

			public void write(NullWritable key, row2Struct value) throws IOException {

				boolean nullValue = value == null;
				if (nullValue) {
					return;
				} else {
					writeObject(value);
				}
			}

			public synchronized void close(Reporter reporter) throws IOException {
				this.m_table.close();
			}
		}

		ContextProperties context;

		public RecordWriter<NullWritable, row2Struct> getRecordWriter(FileSystem ignored, JobConf job, String name,
				Progressable progress) throws IOException {
			context = new ContextProperties(job);
			JobConf hbaseJob = new JobConf(job);
			hbaseJob.set("hbase.zookeeper.quorum", "localhost");
			hbaseJob.set("hbase.zookeeper.property.clientPort", "2181");

			org.apache.hadoop.hbase.client.HTable hTable = new org.apache.hadoop.hbase.client.HTable(hbaseJob, "");
			hTable.setAutoFlush(false);
			return new HBaseTableRecordWriter(hTable, job);
		}

		public void checkOutputSpecs(FileSystem ignored, JobConf job)
				throws FileAlreadyExistsException, InvalidJobConfException, IOException {
		}
	}

	public static class tHBaseOutput_1_ForeachRDD implements
			org.apache.spark.api.java.function.Function<org.apache.spark.api.java.JavaPairRDD<NullWritable, row2Struct>, Void> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -8870960619676428118L;
		private JobConf job;

		public tHBaseOutput_1_ForeachRDD(JobConf job) {
			this.job = job;
		}

		public Void call(org.apache.spark.api.java.JavaPairRDD<NullWritable, row2Struct> rdd) throws Exception {
			rdd.saveAsHadoopDataset(job);
			return null;
		}
	}

	/**
	 * [tHBaseOutput_1 sparkstreamingcode ] stop
	 */
	public void tFileInputDelimited_1_HDFSInputFormatPostProcess(
			final org.apache.spark.streaming.api.java.JavaStreamingContext ctx, GlobalVar globalMap)
					throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
		final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {
			throw e;
		}
	}

	public void tFileInputDelimited_1_HDFSInputFormatProcess(
			final org.apache.spark.streaming.api.java.JavaStreamingContext ctx, GlobalVar globalMap)
					throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
		final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

		try {
			/**
			 * [tFileInputDelimited_1_HDFSInputFormat sparkstreamingconfig ]
			 * start
			 */
			currentComponent = "tFileInputDelimited_1_HDFSInputFormat";

			org.apache.spark.api.java.JavaPairRDD<NullWritable, row_tFileInputDelimited_1_HDFSInputFormatStruct> rdd_row_tFileInputDelimited_1_HDFSInputFormat = ctx
					.sparkContext().hadoopRDD(job, row_tFileInputDelimited_1_HDFSInputFormatStructInputFormat.class,
							NullWritable.class, row_tFileInputDelimited_1_HDFSInputFormatStruct.class);
							/**
							 * [tFileInputDelimited_1_HDFSInputFormat
							 * sparkstreamingconfig ] stop
							 */
							/**
							 * [tFileInputDelimited_1_ExtractDelimited
							 * sparkstreamingconfig ] start
							 */
			currentComponent = "tFileInputDelimited_1_ExtractDelimited";

			org.apache.spark.api.java.JavaPairRDD<NullWritable, row1Struct> rdd_row1 = rdd_row_tFileInputDelimited_1_HDFSInputFormat
					.flatMapToPair(new tFileInputDelimited_1_ExtractDelimited_Function(job));
					/**
					 * [tFileInputDelimited_1_ExtractDelimited
					 * sparkstreamingconfig ] stop
					 */
					/**
					 * [tFilterColumns_1 sparkstreamingconfig ] start
					 */
			currentComponent = "tFilterColumns_1";

			org.apache.spark.api.java.JavaPairRDD<NullWritable, row2Struct> rdd_row2 = rdd_row1
					.mapToPair(new tFilterColumns_1_Function(job));
					/**
					 * [tFilterColumns_1 sparkstreamingconfig ] stop
					 */
					/**
					 * [tHBaseOutput_1 sparkstreamingconfig ] start
					 */
			currentComponent = "tHBaseOutput_1";

			job.setOutputFormat(tHBaseOutput_1StructOutputFormat.class);
			rdd_row2.foreach(new tHBaseOutput_1_ForeachRDD(job));
			/**
			 * [tHBaseOutput_1 sparkstreamingconfig ] stop
			 */
		} catch (java.lang.Exception e) {

			throw e;
		}
	}

	public static class TalendKryoRegistrator implements org.apache.spark.serializer.KryoRegistrator {

		public void registerClasses(com.esotericsoftware.kryo.Kryo kryo) {
			try {
				kryo.register(Class.forName("org.talend.bigdata.dataflow.keys.JoinKeyRecord"));
			} catch (java.lang.ClassNotFoundException e) {
				// Ignore
			}

			kryo.register(java.net.InetAddress.class, new InetAddressSerializer());
			kryo.addDefaultSerializer(java.net.InetAddress.class, new InetAddressSerializer());

			kryo.register(local_project.sparkdemo1_0_1.row_tFileInputDelimited_1_HDFSInputFormatStruct.class);

			kryo.register(local_project.sparkdemo1_0_1.row1Struct.class);

			kryo.register(local_project.sparkdemo1_0_1.row2Struct.class);

		}
	}

	public static class InetAddressSerializer extends com.esotericsoftware.kryo.Serializer<java.net.InetAddress> {

		@Override
		public void write(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Output output,
				java.net.InetAddress value) {
			output.writeInt(value.getAddress().length);
			output.writeBytes(value.getAddress());
		}

		@Override
		public java.net.InetAddress read(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Input input,
				Class<java.net.InetAddress> paramClass) {
			java.net.InetAddress inetAddress = null;
			try {
				int length = input.readInt();
				byte[] address = input.readBytes(length);
				inetAddress = java.net.InetAddress.getByAddress(address);
			} catch (java.net.UnknownHostException e) {
				// Cannot recreate InetAddress instance : return null
			} catch (com.esotericsoftware.kryo.KryoException e) {
				// Should not happen since write() and read() methods are
				// consistent, but if it does happen, it is an unrecoverable
				// error.
				throw new RuntimeException(e);
			}
			return inetAddress;
		}
	}

	public String resuming_logs_dir_path = null;
	public String resuming_checkpoint_path = null;
	public String parent_part_launcher = null;
	public String pid = "0";
	public String rootPid = null;
	public String fatherPid = null;
	public Integer portStats = null;
	public String clientHost;
	public String defaultClientHost = "localhost";
	public String libjars = null;
	private boolean execStat = true;
	public boolean isChildJob = false;
	public String fatherNode = null;
	public String log4jLevel = "";
	public boolean doInspect = false;

	public String contextStr = "Default";
	public boolean isDefaultContext = true;

	private java.util.Properties context_param = new java.util.Properties();
	public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();

	public String status = "";

	public static void main(String[] args) throws java.lang.RuntimeException {
		int exitCode = new sparkdemo1().runJobInTOS(args);

		if (exitCode == 0) {
			System.exit(exitCode);
		} else {
			throw new java.lang.RuntimeException("TalendJob: 'sparkdemo1' - Failed with exit code: " + exitCode + ".");
		}

	}

	public String[][] runJob(String[] args) {
		int exitCode = runJobInTOS(args);
		String[][] bufferValue = new String[][] { { Integer.toString(exitCode) } };
		return bufferValue;
	}

	public int runJobInTOS(String[] args) {
		normalizeArgs(args);

		String lastStr = "";
		for (String arg : args) {
			if (arg.equalsIgnoreCase("--context_param")) {
				lastStr = arg;
			} else if (lastStr.equals("")) {
				evalParam(arg);
			} else {
				evalParam(lastStr + " " + arg);
				lastStr = "";
			}
		}

		initContext();

		if (clientHost == null) {
			clientHost = defaultClientHost;
		}

		if (pid == null || "0".equals(pid)) {
			pid = TalendString.getAsciiRandomString(6);
		}

		if (rootPid == null) {
			rootPid = pid;
		}
		if (fatherPid == null) {
			fatherPid = pid;
		} else {
			isChildJob = true;
		}

		if (portStats != null) {
			// portStats = -1; //for testing
			if (portStats < 0 || portStats > 65535) {
				// issue:10869, the portStats is invalid, so this client socket
				// can't open
				System.err.println("The statistics socket port " + portStats + " is invalid.");
				execStat = false;
			}
		} else {
			execStat = false;
		}

		if (execStat) {
			try {
				runStat.startThreadStat(clientHost, portStats);
				runStat.setAllPID(rootPid, fatherPid, pid);
			} catch (java.io.IOException ioException) {
				ioException.printStackTrace();
			}
		}
		if (!"".equals("liuwu")) {
			System.setProperty("HADOOP_USER_NAME", "liuwu");
		}
		String osName = System.getProperty("os.name");
		String snappyLibName = "libsnappyjava.so";
		if (osName.startsWith("Windows")) {
			snappyLibName = "snappyjava.dll";
		} else if (osName.startsWith("Mac")) {
			snappyLibName = "libsnappyjava.jnilib";
		}
		System.setProperty("org.xerial.snappy.lib.name", snappyLibName);
		try {
			java.util.Map<String, String> tuningConf = new java.util.HashMap<String, String>();
			org.apache.spark.SparkConf sparkConfiguration = getConf(tuningConf);
			org.apache.spark.streaming.api.java.JavaStreamingContext ctx = new org.apache.spark.streaming.api.java.JavaStreamingContext(
					sparkConfiguration, new org.apache.spark.streaming.Duration(1000));

			return run(ctx);
		} catch (Exception ex) {
			ex.printStackTrace();
			ex.printStackTrace(errorMessagePS);
			this.status = "failure";
			return 1;
		}
	}

	/**
	 *
	 * This method runs the Spark job using the SparkContext in argument.
	 * 
	 * @param ctx
	 *            , the SparkContext.
	 * @return the Spark job exit code.
	 *
	 */
	private int run(org.apache.spark.streaming.api.java.JavaStreamingContext ctx) throws java.lang.Exception {
		ctx.addStreamingListener(new TalendSparkStreamingListener(ctx.ssc()));

		initContext();

		setContext(ctx.sparkContext().hadoopConfiguration(), ctx.sparkContext());

		if (doInspect) {
			System.out.println("== inspect start ==");
			System.out.println("{");
			System.out.println("  \"SPARK_MASTER\": \"" + ctx.sparkContext().getConf().get("spark.master") + "\",");
			System.out.println(
					"  \"SPARK_UI_PORT\": \"" + ctx.sparkContext().getConf().get("spark.ui.port", "4040") + "\",");
			System.out
					.println("  \"JOB_NAME\": \"" + ctx.sparkContext().getConf().get("spark.app.name", jobName) + "\"");
			System.out.println("}"); //$NON-NLS-1$
			System.out.println("== inspect end ==");
		}
		globalMap = new GlobalVar(ctx.sparkContext().hadoopConfiguration());
		tFileInputDelimited_1_HDFSInputFormatProcess(ctx, globalMap);
		ctx.start();
		ctx.awaitTermination();
		tFileInputDelimited_1_HDFSInputFormatPostProcess(ctx, globalMap);

		return 0;
	}

	/**
	 *
	 * This method has the responsibility to return a Spark configuration for
	 * the Spark job to run.
	 * 
	 * @return a Spark configuration.
	 *
	 */
	private org.apache.spark.SparkConf getConf(java.util.Map<String, String> tuningConf) throws java.lang.Exception {
		org.apache.spark.SparkConf sparkConfiguration = new org.apache.spark.SparkConf();
		sparkConfiguration.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConfiguration.set("spark.kryo.registrator", TalendKryoRegistrator.class.getName());

		java.text.DateFormat outdfm = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
		String appName = projectName + "_" + jobName + "_" + jobVersion + "_" + outdfm.format(new java.util.Date());
		sparkConfiguration.setAppName(appName);
		sparkConfiguration.setMaster("yarn-client");
		routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();
		java.util.List<String> listJar = new java.util.ArrayList<String>();
		if (libjars != null) {
			for (String jar : libjars.split(",")) {
				listJar.add(jar);
			}
		}
		listJar.add(getJarsToRegister.replaceJarPaths("./" + "sparkdemo1_0_1" + ".jar", "file://"));

		routines.system.BigDataUtil.installWinutils("/tmp",
				getJarsToRegister.replaceJarPaths("../lib/winutils-hadoop-2.6.0.exe", "file://"));
		sparkConfiguration.setJars(listJar.toArray(new String[listJar.size()]));
		sparkConfiguration.set("spark.hadoop.yarn.application.classpath",
				"$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$YARN_HOME/*,$YARN_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,$HADOOP_COMMON_HOME/share/hadoop/common/*,$HADOOP_COMMON_HOME/share/hadoop/common/lib/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,$HADOOP_YARN_HOME/share/hadoop/yarn/*,$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*");
		sparkConfiguration.set("spark.hadoop.yarn.resourcemanager.address", "SAS01:8030");
		if (!"".equals("liuwu")) {
			throw new RuntimeException("The HDFS and the Spark configurations must have the same user name.");
		}
		if (!"".equals("")) {
			System.setProperty("HADOOP_USER_NAME", "");
		}
		tuningConf.put("spark.hadoop.fs.defaultFS", "hdfs://ecX-XXX-XXX-XXX-XXX.compute-XXX.amazonaws.com:9000/");
		tuningConf.put("spark.hadoop.dfs.client.use.datanode.hostname", "true");
		sparkConfiguration.setAll(scala.collection.JavaConversions.asScalaMap(tuningConf));
		sparkConfiguration.set("spark.local.dir", "/tmp");

		return sparkConfiguration;
	}

	private String genTempFolderForComponent(String name) {
		java.io.File tempDir = new java.io.File("/tmp/" + pid, name);
		String tempDirPath = tempDir.getPath();
		if (java.io.File.separatorChar != '/')
			tempDirPath = tempDirPath.replace(java.io.File.separatorChar, '/');
		return tempDirPath;
	}

	private void initContext() {
		// get context
		try {
			// call job/subjob with an existing context, like:
			// --context=production. if without this parameter, there will use
			// the default context instead.
			java.io.InputStream inContext = sparkdemo1.class.getClassLoader()
					.getResourceAsStream("local_project/sparkdemo1_0_1/contexts/" + contextStr + ".properties");
			if (isDefaultContext && inContext == null) {

			} else {
				if (inContext != null) {
					// defaultProps is in order to keep the original context
					// value
					defaultProps.load(inContext);
					inContext.close();
					context = new ContextProperties(defaultProps);
				} else {
					// print info and job continue to run, for case:
					// context_param is not empty.
					System.err.println("Could not find the context " + contextStr);
				}
			}

			if (!context_param.isEmpty()) {
				context.putAll(context_param);
			}
			context.loadValue(context_param, null);
			if (parentContextMap != null && !parentContextMap.isEmpty()) {
			}
		} catch (java.io.IOException ie) {
			System.err.println("Could not load context " + contextStr);
			ie.printStackTrace();
		}
	}

	private void setContext(Configuration conf, org.apache.spark.api.java.JavaSparkContext ctx) {
		// get context
		// call job/subjob with an existing context, like: --context=production.
		// if without this parameter, there will use the default context
		// instead.
		java.net.URL inContextUrl = sparkdemo1.class.getClassLoader()
				.getResource("local_project/sparkdemo1_0_1/contexts/" + contextStr + ".properties");
		if (isDefaultContext && inContextUrl == null) {

		} else {
			if (inContextUrl != null) {
				conf.set(ContextProperties.CONTEXT_FILE_NAME, contextStr + ".properties");
				java.io.File contextFile = new java.io.File(inContextUrl.getPath());
				if (contextFile.exists()) {
					ctx.addFile(inContextUrl.getPath());
				} else {
					java.io.InputStream contextIn = sparkdemo1.class.getClassLoader()
							.getResourceAsStream("local_project/sparkdemo1_0_1/contexts/" + contextStr + ".properties");
					if (contextIn != null) {
						java.io.File tmpFile = new java.io.File(contextStr + ".properties");
						java.io.OutputStream contextOut = null;
						try {
							tmpFile.createNewFile();
							contextOut = new java.io.FileOutputStream(tmpFile);

							int len = -1;
							byte[] b = new byte[4096];
							while ((len = contextIn.read(b)) != -1) {
								contextOut.write(b, 0, len);
							}
						} catch (java.io.IOException ioe) {
							ioe.printStackTrace();
						} finally {
							try {
								contextIn.close();
								if (contextOut != null) {
									contextOut.close();
								}
							} catch (java.io.IOException ioe) {
								ioe.printStackTrace();
							}
						}

						ctx.addFile(tmpFile.getPath());
						tmpFile.delete();
					}
				}

			}
		}

		if (!context_param.isEmpty()) {
			for (Object contextKey : context_param.keySet()) {
				conf.set(ContextProperties.CONTEXT_PARAMS_PREFIX + contextKey,
						context.getProperty(contextKey.toString()));
				conf.set(ContextProperties.CONTEXT_KEYS,
						conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + contextKey);
			}
		}

		if (parentContextMap != null && !parentContextMap.isEmpty()) {
		}
	}

	private void evalParam(String arg) {
		if (arg.startsWith("--resuming_logs_dir_path")) {
			resuming_logs_dir_path = arg.substring(25);
		} else if (arg.startsWith("--resuming_checkpoint_path")) {
			resuming_checkpoint_path = arg.substring(27);
		} else if (arg.startsWith("--parent_part_launcher")) {
			parent_part_launcher = arg.substring(23);
		} else if (arg.startsWith("--father_pid=")) {
			fatherPid = arg.substring(13);
		} else if (arg.startsWith("--root_pid=")) {
			rootPid = arg.substring(11);
		} else if (arg.startsWith("--pid=")) {
			pid = arg.substring(6);
		} else if (arg.startsWith("--context=")) {
			contextStr = arg.substring("--context=".length());
			isDefaultContext = false;
		} else if (arg.startsWith("--context_param")) {
			String keyValue = arg.substring("--context_param".length() + 1);
			int index = -1;
			if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
				context_param.put(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
			}
		} else if (arg.startsWith("--stat_port=")) {
			String portStatsStr = arg.substring(12);
			if (portStatsStr != null && !portStatsStr.equals("null")) {
				portStats = Integer.parseInt(portStatsStr);
			}
		} else if (arg.startsWith("--client_host=")) {
			clientHost = arg.substring(14);
		} else if (arg.startsWith("--log4jLevel=")) {
			log4jLevel = arg.substring(13);
		} else if (arg.startsWith("--inspect")) {
			doInspect = Boolean.valueOf(arg.substring("--inspect=".length()));
		}
	}

	private void normalizeArgs(String[] args) {
		java.util.List<String> argsList = java.util.Arrays.asList(args);
		int indexlibjars = argsList.indexOf("-libjars") + 1;
		libjars = indexlibjars == 0 ? null : argsList.get(indexlibjars);
	}

	private final String[][] escapeChars = { { "\\\\", "\\" }, { "\\n", "\n" }, { "\\'", "\'" }, { "\\r", "\r" },
			{ "\\f", "\f" }, { "\\b", "\b" }, { "\\t", "\t" } };

	private String replaceEscapeChars(String keyValue) {

		if (keyValue == null || ("").equals(keyValue.trim())) {
			return keyValue;
		}

		StringBuilder result = new StringBuilder();
		int currIndex = 0;
		while (currIndex < keyValue.length()) {
			int index = -1;
			// judege if the left string includes escape chars
			for (String[] strArray : escapeChars) {
				index = keyValue.indexOf(strArray[0], currIndex);
				if (index >= 0) {

					result.append(keyValue.substring(currIndex, index + strArray[0].length()).replace(strArray[0],
							strArray[1]));
					currIndex = index + strArray[0].length();
					break;
				}
			}
			// if the left string doesn't include escape chars, append the left
			// into the result
			if (index < 0) {
				result.append(keyValue.substring(currIndex));
				currIndex = currIndex + keyValue.length();
			}
		}

		return result.toString();
	}

	public String getStatus() {
		return status;
	}

}
