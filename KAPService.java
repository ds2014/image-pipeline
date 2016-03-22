package edu.umd.lims.fedora.kap;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import fedora.client.FedoraClient;
import fedora.client.Uploader;
import fedora.server.access.FedoraAPIA;
import fedora.server.management.FedoraAPIM;

public class KAPService {

	private static final Logger log = LoggerFactory.getLogger(KAPService.class);

	public final static long DEFAULT_SHUTDOWN_WAIT = 60;
	private static FedoraClient client = null;
	private static FedoraAPIA APIA = null;
	private static FedoraAPIM APIM = null;
	private static Uploader uploader = null;
	private static Properties context = null;
	private static KAPService service;
	private static String host = null;
	private static int port = 80;
	private static String user;
	private static String password;
	private final static StopWatch timer = new StopWatch();
	private static int maxRecordCount = 0;
	private static int batchSize = 5;
	private static int sampleProbeSize = 2;
	private static String dataPath = null;
	private static String metadataPath = null;
	private static String metadataStagedPath = null;
	private static String metadataProcessedPath = null;
	private static String logsPath = null;
	private static String outputProcessedPath = null;
	private static String errorsLogPath = null;

	static ICsvMapWriter errorLogWriter = null;
	static ICsvMapWriter appLogWriter = null;
	static ICsvMapWriter batchStatusWriter = null;
	static ICsvMapWriter appStatusWriter = null;
	static ICsvMapWriter appStatsWriter = null;

	private static String stagedPartitionsPath = null;
	private static String processedPartitionsPath = null;
	private static String batchPath = null;
	
	private static String partitionFolder = null;

	
	static ExecutorService umdmPool = new LoaderThreadPool(
			1, // core thread pool size
			1, // maximum thread pool size
			1, // time to wait before resizing pool
			TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1, true),
			new ThreadPoolFactory("UMDM Pool"));

	private static ExecutorService umamPool = new LoaderThreadPool(
			1, // core thread pool size
			1, // maximum thread pool size
			1, // time to wait before resizing pool
			TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1, true),
			new ThreadPoolFactory("UMAM Pool"));

	private static ExecutorService uploadPool = new LoaderThreadPool(
			1, // core thread pool size
			1, // maximum thread pool size
			1, // time to wait before resizing pool
			TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1, true),
			new ThreadPoolFactory("Upload Pool"));
	
	private KAPService() {

	}

	private static class KAPServiceHolder {
		public static final KAPService INSTANCE = new KAPService();
	}

	public static KAPService getInstance() {
		if (context == null) {
			context = UtilsProperties.getProperties();
		}
		return KAPServiceHolder.INSTANCE;
	}

	public static KAPService getInstance(Properties properties) {
		context = properties;
		return KAPServiceHolder.INSTANCE;
	}

	public static void initialize() throws IOException, KAPServiceException {
		service = KAPServiceHolder.INSTANCE;

		log.info("Initializing loading service...");
		if (context == null) {

			log.info("Initializing context properties...");
			context = UtilsProperties.getProperties();
		}

		initHost();

		initClient();

		if (initFedoraAPI()) {
			log.info("Fedora API has been initialized.");
			initUploader();
			log.info("Fedora Uploader has been initialized.");
		}

		initDataSource();
		log.info("Loading  Service has been initialized.");

		initLogWriters();
	}

	private static void initClient() throws IOException, KAPServiceException {

		log.info("Initializing Fedora client...");

		String connection = getConnection();
		log.info("Fedora Client Connection: " + connection);

		user = context.getProperty("fedora.user");
		password = context.getProperty("fedora.password");
		log.info("Connecting as user: " + user);

		FedoraClient.FORCE_LOG4J_CONFIGURATION = false;

		client = new FedoraClient(connection, user, password);

		if (!canConnect()) {
			throw new KAPServiceException("Error connecting to Fedora client: "
					+ getConnection(),
					ServiceErrorCode.FEDORA_API_ERROR.getValue());
		}

		// TCP connection timeout 10 min (establishment of the TCP connection)
		client.getHttpClient().getHttpConnectionManager().getParams()
				.setConnectionTimeout(10 * 60 * 1000);
		// Socket timeout 10 min (generate connection timeout) if there is no
		// incomimg data flow within 10 min
		client.getHttpClient().getHttpConnectionManager().getParams()
				.setSoTimeout(10 * 60 * 1000);
		// Set max connection count
		client.getHttpClient().getHttpConnectionManager().getParams()
				.setMaxTotalConnections(10);

		log.info("Fedora client has been inialized. Fedora client endpoint url: "
				+ client.getUploadURL());

	}

	public static int getFedoraClientStatus() {

		GetMethod getMethod = new GetMethod(getConnection());
		int responseCode = 0;

		try {
			if (client != null) {
				responseCode = client.getHttpClient().executeMethod(getMethod);
			} else {
				log.error("Fedora client has not been initialized.");
			}
		} catch (IOException e) {
			log.error("Cannot get Fedora client status");
		}

		log.info("Client response code: " + responseCode);

		return responseCode;
	}

	public static boolean canConnect() {

		boolean result = false;
		if (getFedoraClientStatus() == HttpStatus.SC_OK) {
			result = true;
		}

		log.info("Fedora client connection status OK: " + result);

		return result;
	}

	public static void initClientConnection() throws IOException,
			KAPServiceException {

		initClient();

		if (initFedoraAPI()) {
			log.info("Fedora API has been initialized.");
			initUploader();
			log.info("Fedora Uploader has been initialized.");
		}

	}

	private static boolean initFedoraAPI() throws KAPServiceException {
		boolean result = true;
		try {
			if ((client != null) && canConnect()) {

				if (APIA == null) {
					APIA = client.getAPIA();
				}

				if (APIM == null) {
					APIM = client.getAPIM();
				}

			} else {
				result = false;
				throw new KAPServiceException("Cannot instantiate Fedora API"
						+ ServiceErrorCode.FEDORA_CLIENT_EMPTY.getValue(),
						ServiceErrorCode.FEDORA_CLIENT_EMPTY.getValue());
			}
		} catch (Exception e) {
			throw new KAPServiceException(e.getMessage(),
					ServiceErrorCode.FEDORA_API_ERROR.getValue());
		}
		return result;
	}

	public static void initUploader() throws KAPServiceException {
		try {
			log.info("Initializing Uploader: " + "Host: " + host + " Port: "
					+ port);
			uploader = new Uploader("http", host, port, user, password);
		} catch (IOException e) {
			throw new KAPServiceException(e.getMessage(),
					ServiceErrorCode.FEDORA_UPLOADER_ERROR.getValue());
		}
	}

	private static void initDataSource() throws KAPServiceException {

		createBatchDirectory();

		if (NumberUtils.isNumber(context.getProperty("batch.size").trim())) {
			batchSize = Integer.parseInt(context.getProperty("batch.size")
					.trim());
		}

		if (NumberUtils.isNumber(context.getProperty("sample.probe.size")
				.trim())) {
			sampleProbeSize = Integer.parseInt(context.getProperty(
					"sample.probe.size").trim());
		}

		if (NumberUtils.isNumber(context.getProperty("maxrecords").trim())) {
			maxRecordCount = Integer.parseInt(context.getProperty("maxrecords")
					.trim());
		}

		dataPath = context.getProperty("data.path");
		metadataPath = context.getProperty("master.path");
		metadataStagedPath = context.getProperty("master.staged.path");
		metadataProcessedPath = context.getProperty("master.processed.path");
		outputProcessedPath = context.getProperty("output.processed.path");
		partitionFolder = context.getProperty("partition.folder");

		logsPath = context.getProperty("logs.path");

		if ((dataPath == null)
				|| Files.notExists(Paths.get(getDataPath()))
				|| !(Files.isDirectory(Paths.get(getDataPath())) || !Files
						.isReadable(Paths.get(getDataPath())))) {
			throw new KAPServiceException("Error reading data directory",
					ServiceErrorCode.DATA_PATH_ERROR.getValue());
		}

		if ((metadataPath == null)
				|| (Files.notExists(Paths.get(getMetadataPath())))) {
			throw new KAPServiceException("Error reading KAP metadata",
					ServiceErrorCode.MD_PATH_ERROR.getValue());
		}

		if (metadataStagedPath == null) {
			throw new KAPServiceException(
					"Please, define a path to the staged data file.",
					ServiceErrorCode.MD_PATH_ERROR.getValue());
		}

		if (metadataProcessedPath == null) {
			throw new KAPServiceException(
					"Please, define a path to the processed data file.",
					ServiceErrorCode.MD_PATH_ERROR.getValue());
		}

		if (outputProcessedPath == null) {
			throw new KAPServiceException(
					"Please, define a path to the output processed data file.",
					ServiceErrorCode.MD_PATH_ERROR.getValue());
		}

		if (((logsPath == null) || (Files.notExists(Paths.get(getLogsPath())))
				|| (!Files.isDirectory(Paths.get(getLogsPath()))) || !Files
					.isReadable(Paths.get(getLogsPath())))) {
			throw new KAPServiceException(
					"Error reading application logs path",
					ServiceErrorCode.LOG_PATH_ERROR.getValue());
		}

		// partitioning & batch related properties

		if (batchPath == null) {
			throw new KAPServiceException(
					"Cannot identify a path to a current batch",
					ServiceErrorCode.BATCH_DIR_NOT_EXIST.getValue());
		}

		if (stagedPartitionsPath == null) {
			throw new KAPServiceException(
					"Please, define a path to the staged partitions",
					ServiceErrorCode.LOG_PATH_ERROR.getValue());
		}

		if (processedPartitionsPath == null) {
			throw new KAPServiceException(
					"Please, define a path to the processed results of each partition.",
					ServiceErrorCode.LOG_PATH_ERROR.getValue());
		}

		if (errorsLogPath == null) {
			throw new KAPServiceException(
					"Cannot initilaize errors log path for a batch.",
					ServiceErrorCode.LOG_PATH_ERROR.getValue());
		}

		log.info("Data path: " + getDataPath());
		log.info("Metadata file path: " + getMetadataPath());
		log.info("Metadata Staged path: " + getMetadataStagedPath());
		log.info("Metadata Processed path: " + getMetadataProcessedPath());
		log.info("Application Logs path: " + getLogsPath());
		log.info("Output processed data path: " + getOutputProcessedPath());

		log.info("Batch root directory path: " + getBatchPath());
		log.info("Batch Errors Log path: " + getErrorsLogPath());
		log.info("Staged partitions path: " + getStagedPartitionsPath());
		log.info("Processed partitions path: " + getProcessedPartitionsPath());

		log.info("Partition size: " + getBatchSize());
		log.info("Sample probe size: " + getSampleProbeSize());
		log.info("Max Record Count: " + getMaxRecordCount());

	}

	private static void initHost() throws KAPServiceException {

		host = context.getProperty("fedora.host");

		if (host == null) {
			throw new KAPServiceException("Host cannot be null",
					ServiceErrorCode.HOST_ERROR.getValue());
		}

		port = NumberUtils.toInt(context.getProperty("fedora.port"));
	}

	public static String getConnection() {

		log.debug("Connection: Fedora Host: " + getHost() + " Fedora Port: "
				+ getPort());
		String connection = "http://" + getHost() + ":" + getPort() + "/fedora";

		return connection;
	}

	public static int getMaxRecordCount() {
		return maxRecordCount;
	}

	public static void initLogWriters() throws KAPServiceException {

		FileWriter batchLogfileWriter;
		FileWriter aplicationLogFileWriter;
		FileWriter appStatsFileWriter;

		try {

			// log writer for a batch
			batchLogfileWriter = new FileWriter(getErrorsLogPath()
					+ "/errors.csv");

			errorLogWriter = new CsvMapWriter(batchLogfileWriter,
					CsvPreference.STANDARD_PREFERENCE);

			// write the error log header
			errorLogWriter.writeHeader(getErrorLogHeader());

			// application log writer
			aplicationLogFileWriter = new FileWriter(getLogsPath()
					+ "/application-log.csv");

			appLogWriter = new CsvMapWriter(aplicationLogFileWriter,
					CsvPreference.STANDARD_PREFERENCE);

			// write the error log header
			appLogWriter.writeHeader(getErrorLogHeader());

			appStatsFileWriter = new FileWriter(getErrorsLogPath()
					+ "/stats-summary.csv");

			// current stats writer
			appStatsWriter = new CsvMapWriter(appStatsFileWriter,
					CsvPreference.STANDARD_PREFERENCE);

			// write the app stats log
			appStatsWriter.writeHeader(getStatsLogHeader());

		} catch (IOException e) {

			throw new KAPServiceException("Error initializing log writers.",
					ServiceErrorCode.LOG_PATH_ERROR.getValue());

		}

	}

	public static void closeLogWriters() {

		if (errorLogWriter != null) {
			try {
				errorLogWriter.flush();
				errorLogWriter.close();
				log.info("Error log has been written and closed by the following path: "
						+ getErrorsLogPath());
			} catch (IOException e) {
				log.error("Error closing error log writer by the following path: "
						+ getErrorsLogPath());
			}

		}

		if (appLogWriter != null) {
			try {
				appLogWriter.flush();
				appLogWriter.close();
				log.info("Application log has been written and closed by the following path: "
						+ getLogsPath());
			} catch (IOException e) {
				log.error("Application closing error log writer by the following path: "
						+ getLogsPath());
			}

		}

		if (appStatsWriter != null) {
			try {
				appStatsWriter.flush();
				appStatsWriter.close();
				log.info("Application stats has been written and closed by the following path: "
						+ getErrorsLogPath() + "/stats-summary.csv");
			} catch (IOException e) {
				log.error("Application stats closing error writer by the following path: "
						+ getErrorsLogPath() + "/stats-summary.csv");
			}

		}

	}

	public static void writeErrorLog(ErrorLogEntry error) {

		final Map<String, Object> logEntry = new HashMap<String, Object>();
		logEntry.put(getErrorLogHeader()[0], error.getErrorId());
		logEntry.put(getErrorLogHeader()[1], error.getMessage());
		logEntry.put(getErrorLogHeader()[2],
				DateUtils.getFormattedCurrentTime());

		try {
			errorLogWriter.write(logEntry, getErrorLogHeader(),
					getLogErrorProcessors());
			errorLogWriter.flush();

		} catch (IOException e) {
			log.error("Cannot write to error log writer by the following path: "
					+ getErrorsLogPath());
		}

	}

	public static void writeAppLog(ErrorLogEntry error) {

		final Map<String, Object> logEntry = new HashMap<String, Object>();
		logEntry.put(getErrorLogHeader()[0], error.getErrorId());
		logEntry.put(getErrorLogHeader()[1], error.getMessage());
		logEntry.put(getErrorLogHeader()[2],
				DateUtils.getFormattedCurrentTime());

		try {
			appLogWriter.write(logEntry, getErrorLogHeader(),
					getLogErrorProcessors());
			appLogWriter.flush();

		} catch (IOException e) {
			log.error("Cannot write to application log writer by the following path: "
					+ getLogsPath());
		}

	}

	public static void writeStatsEntry(String statEntry) {

		final Map<String, Object> logEntry = new HashMap<String, Object>();
		logEntry.put(getStatsLogHeader()[0], statEntry);
		logEntry.put(getStatsLogHeader()[1],
				DateUtils.getFormattedCurrentTime());

		log.info("Stat entry " + logEntry);
		try {
			appStatsWriter.write(logEntry, getStatsLogHeader(),
					getStatsLogProcessors());
			appStatsWriter.flush();

		} catch (IOException e) {
			log.error("Cannot write to stats log writer by the following path: "
					+ getErrorsLogPath() + "/stats-summary.csv");
		}

	}

	public static void writeBatchStatus(StatusEntry entry) {

		FileWriter batchStatusFileWriter = null;
		FileWriter lastBatchStatusFileWriter = null;

		try {

			batchStatusFileWriter = new FileWriter(getErrorsLogPath()
					+ "/batch-status.csv");
			lastBatchStatusFileWriter = new FileWriter(getLogsPath()
					+ "/last-processed-batch.csv");

			batchStatusWriter = new CsvMapWriter(batchStatusFileWriter,
					CsvPreference.STANDARD_PREFERENCE);

			appStatusWriter = new CsvMapWriter(lastBatchStatusFileWriter,
					CsvPreference.STANDARD_PREFERENCE);

			final Map<String, Object> logEntry = new HashMap<String, Object>();

			logEntry.put(getBatchLogHeader()[0], entry.getBatchPath());
			logEntry.put(getBatchLogHeader()[1], entry.getEnd());
			logEntry.put(getBatchLogHeader()[2], entry.getStatus());
			logEntry.put(getBatchLogHeader()[3],
					DateUtils.getFormattedCurrentTime());

			batchStatusWriter.write(logEntry, getBatchLogHeader(),
					getBatchLogProcessors());
			batchStatusWriter.flush();

			appStatusWriter.write(logEntry, getBatchLogHeader(),
					getBatchLogProcessors());
			appStatusWriter.flush();

		} catch (IOException e) {
			log.error("Cannot write batch/last-batch status.");
		} finally {
			if (batchStatusWriter != null) {
				try {
					batchStatusWriter.flush();
					batchStatusWriter.close();
					log.info("Batch status has been written path: "
							+ getErrorsLogPath() + "/batch-status.csv");
				} catch (IOException e) {
					log.error("Error closing batch status writer by the following path: "
							+ getErrorsLogPath() + "/batch-status.csv");
				}

			}

			if (appStatusWriter != null) {
				try {
					appStatusWriter.flush();
					appStatusWriter.close();
					log.info("Last batch status has been written path: "
							+ getLogsPath() + "/last-processed-batch.csv");
				} catch (IOException e) {
					log.error("Error closing last batch status writer by the following path: "
							+ getLogsPath() + "/last-processed-batch.csv");
				}

			}

		}

	}

	public static KAPService getService() {
		return getInstance();
	}

	public static FedoraClient getFedoraClient() {
		return client;
	}

	public static FedoraAPIA getAPIA() {
		return APIA;
	}

	public static FedoraAPIM getAPIM() {
		return APIM;
	}

	public static Uploader getUploader() {
		return uploader;
	}

	public static String getHost() {
		if (host != null) {
			return host.trim();
		} else {
			return null;
		}
	}

	public static int getPort() {
		return port;
	}

	public static String getDataPath() {

		if (dataPath != null) {
			return dataPath.trim();
		} else {
			return null;
		}

	}

	public static String getMetadataPath() {

		if (metadataPath != null) {
			return metadataPath.trim();
		} else {
			return null;
		}
	}

	public static String getMetadataStagedPath() {

		if (metadataStagedPath != null) {
			return metadataStagedPath.trim();
		} else {
			return null;
		}

	}

	public static String getBatchPath() {
		return batchPath;
	}

	public static String getMetadataProcessedPath() {

		if (metadataProcessedPath != null) {
			return metadataProcessedPath.trim();
		} else {
			return null;
		}
	}

	public static String getLogsPath() {

		if (logsPath != null) {
			return logsPath.trim();
		} else {
			return null;
		}

	}

	public static int getBatchSize() {
		return batchSize;
	}

	public static int getSampleProbeSize() {
		return sampleProbeSize;
	}

	public static String getOutputProcessedPath() {

		if (outputProcessedPath != null) {
			return outputProcessedPath.trim();
		} else {
			return null;
		}

	}

	public static String getErrorsLogPath() {

		if (errorsLogPath != null) {
			return errorsLogPath.trim();
		} else {
			return null;
		}
	}

	public static String getStagedPartitionsPath() {

		if (stagedPartitionsPath != null) {
			return stagedPartitionsPath.trim();
		} else {
			return null;
		}

	}

	public static String getProcessedPartitionsPath() {

		if (processedPartitionsPath != null) {
			return processedPartitionsPath.trim();
		} else {
			return null;
		}

	}

	public static String getPartitionFolder() {

		if (partitionFolder != null) {
			return partitionFolder.trim();
		} else {
			return null;
		}

	}

	private static String[] getErrorLogHeader() {

		final String[] header = new String[] { "error_id", "message",
				"timestamp" };
		return header;
	}

	private static String[] getStatsLogHeader() {

		final String[] header = new String[] { "message", "timestamp" };
		return header;
	}

	private static String[] getBatchLogHeader() {

		final String[] header = new String[] { "batch_path", "end", "status",
				"timestamp" };
		return header;
	}

	public static CellProcessor[] getLogErrorProcessors() {

		final CellProcessor[] processors = new CellProcessor[] { new NotNull(), // error_id
				new NotNull(), // message
				new NotNull() // timestamp
		};

		return processors;
	}

	public static CellProcessor[] getBatchLogProcessors() {

		final CellProcessor[] processors = new CellProcessor[] { new NotNull(), // batch_path
				new Optional(), // end
				new NotNull(), // status
				new NotNull(), // timestamp
		};

		return processors;
	}

	public static CellProcessor[] getStatsLogProcessors() {

		final CellProcessor[] processors = new CellProcessor[] {
				new Optional(), // message
				new Optional() // timestamp
		};

		return processors;
	}

	public static void createBatchDirectory() throws KAPServiceException {

		Exception exception = null;
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy-h-mm-ss-a");
		String currentTime = sdf.format(date);
		Path rootPath = Paths.get(context.getProperty("data.path").trim())
				.getParent();

		String batchName = rootPath + "/" + currentTime;

		Path batchRootPath = Paths.get(batchName);
		Path logPath = Paths.get(batchName + "/logs");
		Path stagedPath = Paths.get(batchName + "/staged");
		Path processedPath = Paths.get(batchName + "/processed");

		if (Files.notExists(batchRootPath)) {
			try {
				// create batch directory
				Files.createDirectory(batchRootPath);
				batchPath = batchRootPath.toFile().getAbsolutePath();
				log.info("Batch directory has been created. "
						+ batchRootPath.toFile().getAbsolutePath());

				// create log directory
				Files.createDirectory(logPath);
				errorsLogPath = logPath.toFile().getAbsolutePath();
				log.info("Logs directory has been created for the batch. ; Batch Name: "
						+ batchName
						+ " ; "
						+ logPath.toFile().getAbsolutePath());

				// create staged directory
				Files.createDirectory(stagedPath);
				stagedPartitionsPath = stagedPath.toFile().getAbsolutePath();
				log.info("Staged directory has been created for the batch. ; Batch Name: "
						+ batchName
						+ " ; "
						+ stagedPath.toFile().getAbsolutePath());

				// create processed directory
				Files.createDirectory(processedPath);
				processedPartitionsPath = processedPath.toFile()
						.getAbsolutePath();
				log.info("Processed directory has been created for the batch. ; Batch Name: "
						+ batchName
						+ " ; "
						+ processedPath.toFile().getAbsolutePath());

			} catch (IOException e) {
				e.printStackTrace();
				exception = e;
			} finally {

				if (exception != null) {
					String errorMessage = "Error while batch directory structure. "
							+ exception.getMessage();
					log.error(errorMessage);

					throw new KAPServiceException(errorMessage,
							ServiceErrorCode.BATCH_DIR_ERROR.getValue());
				}
			}

		}

	}

	public static ExecutorService getUploadPooll() {
		return uploadPool;
	}

	public static ExecutorService getUMDMPooll() {
		return umdmPool;
	}

	public static ExecutorService getUMAMPooll() {
		return umamPool;
	}

	public static String getUser() {
		return user;
	}

	public static String getPassword() {
		return password;
	}

	public static void shutdownPools() {

		uploadPool.shutdownNow();
		umamPool.shutdownNow();
		umdmPool.shutdown();

	}

}
