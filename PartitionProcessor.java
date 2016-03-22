package edu.umd.lims.fedora.kap;

import java.io.IOException;
import java.util.Map;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.io.ICsvMapWriter;

public class PartitionProcessor {

	private static final Logger log = LoggerFactory
			.getLogger(PartitionProcessor.class);

	private Partition partition;
	private final static StopWatch timer = new StopWatch();
	private static UMDMCSVInput umdmTitle = new UMDMCSVInput();
	private UMAMCSVInput umamComponent = null;
	private int maxRecordCount = 0;

	private static String processedUmdmId = null;
	private static String currentUmdmId = null;
	private String umamId = null;

	PartitionProcessor(Partition partition) {
		this.partition = partition;
	}

	public void loadPartition() {
		timer.reset();
		timer.start();

		log.info("Loading Partition :" + partition + "; " + " has started.");

		log.info("Metadata staged input file: "
				+ this.partition.getPartitionPath());

		try {
			this.partition.writePartitionHeaders();

			load();

		} catch (IOException e) {
			String errorMessage = "Error processing Partition " + partition
					+ "; " + this.partition.getPartitionPath() + "; "
					+ "Message: " + e.getMessage() + "; Cause: " + e.getCause();

			log.error(errorMessage);
			e.printStackTrace();

			// log global error
			LoaderStats.errorCount.increment();

			ErrorLogEntry error = new ErrorLogEntry(
					LoaderStats.errorCount.getValue(), errorMessage);
			KAPService.writeErrorLog(error);

			// log local partition error
			this.partition.writeErrorLog(error);
			this.partition.getStats().errorCount.increment();

		}
		timer.stop();

		log.info(partition + "\n"
				+ "partition has been loaded. Total time taken. "
				+ timer.toString());

	}

	private void load() throws IOException {
		ICsvMapReader partitionReader = this.partition.getPartitionReader();
		ICsvMapWriter partitionProcessedWriter = this.partition
				.getProcessedWriter();
		ICsvMapWriter partitionErrorLogWriter = this.partition
				.getProcessedErrorWriter();

		final String[] header = this.partition.getPartitionStagedFilesHeader();
		final CellProcessor[] processors = this.partition
				.getPartitionStagedFilesProcessors();

		Exception exception = null;
		Map<String, Object> partitionMap;

		try {
			while ((partitionMap = partitionReader.read(header, processors)) != null
					&& (LoaderStats.uploadedProcessedUMDMFileCount.getValue() < maxRecordCount)) {
				log.info(String.format("lineNo=%s, rowNo=%s, partitionMap=%s",
						partitionReader.getLineNumber(),
						partitionReader.getRowNumber(), partitionMap));

				currentUmdmId = (String) partitionMap.get("umdm_id");
				umamId = (String) partitionMap.get("umam_page_id");

				try {
					processRow(partitionReader, partitionMap, umamId);
				} catch (Exception e) {
					// Log error and continue to process next line
					String errorMessage = "Error processing input partition UMAM File Metadata , line #:"
							+ partitionReader.getLineNumber()
							+ "; "
							+ partition.getPartitionPath()
							+ "; "
							+ "Message: "
							+ e.getMessage() + "; Cause: " + e.getCause();

					log.error(errorMessage);
					e.printStackTrace();

					// log global error
					LoaderStats.errorCount.increment();
					ErrorLogEntry error = new ErrorLogEntry(
							LoaderStats.errorCount.getValue(), errorMessage);
					KAPService.writeErrorLog(error);

					// log local partition error
					this.partition.getStats().errorCount.increment();
					this.partition.writeErrorLog(error);
				}
			}

			// upload the last UMDM Title
			try {
				if ((umdmTitle != null)
						&& (LoaderStats.uploadedProcessedUMDMFileCount
								.getValue() < maxRecordCount)) {

					if (umdmTitle.getId() != null) {
						uploadTitle(umdmTitle);
					}
				}
				// end uploading the last UMDM Title

			} catch (Exception e) {

				// Log error
				String errorMessage = "Error processing input partition UMAM File Metadata , line #:"
						+ partitionReader.getLineNumber()
						+ "; "
						+ partition.getPartitionPath()
						+ "; "
						+ "Message: "
						+ e.getMessage() + "; Cause: " + e.getCause();
				;

				log.error(errorMessage);
				e.printStackTrace();

				// log global error
				LoaderStats.errorCount.increment();
				ErrorLogEntry error = new ErrorLogEntry(
						LoaderStats.errorCount.getValue(), errorMessage);
				KAPService.writeErrorLog(error);

				// log local partition error
				this.partition.getStats().errorCount.increment();
				this.partition.writeErrorLog(error);
			}

		} catch (IOException e) {

			exception = e;
			String errorMessage = "Error reading input partition UMAM File Metadata."
					+ "; "
					+ partition.getPartitionPath()
					+ "; "
					+ "Message: "
					+ e.getMessage() + "; Cause: " + e.getCause();

			log.error(errorMessage);
			e.printStackTrace();

			// log global error
			LoaderStats.errorCount.increment();
			ErrorLogEntry error = new ErrorLogEntry(
					LoaderStats.errorCount.getValue(), errorMessage);
			KAPService.writeErrorLog(error);

			// log local partition error
			this.partition.getStats().errorCount.increment();
			this.partition.writeErrorLog(error);

		} finally { // close partition readers and writers
			if (partitionReader != null) {
				try {
					partitionReader.close();
				} catch (IOException e) {
					log.error("Error when attempt to close input partition UMAM File Metadata."
							+ partition.getPartitionPath()
							+ "; "
							+ "Message: "
							+ e.getMessage() + "; Cause: " + e.getCause());
				}
			}

			if (partitionProcessedWriter != null) {
				try {
					partitionProcessedWriter.close();
				} catch (IOException e) {

					String errorMessage = "Error when attempt to close output partition processed UMAM File Metadata."
							+ partition.getPartitionProcessedPath()
							+ "; "
							+ "Message: "
							+ e.getMessage()
							+ "; Cause: "
							+ e.getCause()

							+ e.getMessage();

					log.error(errorMessage);
					e.printStackTrace();

					// log global error
					LoaderStats.errorCount.increment();
					ErrorLogEntry error = new ErrorLogEntry(
							LoaderStats.errorCount.getValue(), errorMessage);
					KAPService.writeErrorLog(error);

					// log local partition error
					this.partition.getStats().errorCount.increment();
					this.partition.writeErrorLog(error);
				}
			}

			if (partitionErrorLogWriter != null) {
				try {
					partitionErrorLogWriter.close();
				} catch (IOException e) {

					String errorMessage = "Error when attempt to close partition error log file."
							+ partition.getPartitionPath()
							+ ";  "
							+ "Message: "
							+ exception.getMessage()
							+ "; Cause: "
							+ exception.getCause()
							+ e.getMessage();

					log.error(errorMessage);
					e.printStackTrace();

					// log global error
					LoaderStats.errorCount.increment();
					ErrorLogEntry error = new ErrorLogEntry(
							LoaderStats.errorCount.getValue(), errorMessage);
					KAPService.writeErrorLog(error);

					// log local partition error
					this.partition.getStats().errorCount.increment();
					this.partition.writeErrorLog(error);
				}
			}

			// report error processing current partition to global error log
			if (exception != null) {
				String errorMessage = "Error reading input partition UMAM File Metadata. "
						+ partition.getPartitionPath()
						+ ";  "
						+ "Message: "
						+ exception.getMessage()
						+ "; Cause: "
						+ exception.getCause();

				log.error(errorMessage);
				exception.printStackTrace();

				LoaderStats.errorCount.increment();
				ErrorLogEntry error = new ErrorLogEntry(
						LoaderStats.errorCount.getValue(), errorMessage);
				KAPService.writeErrorLog(error);

				// log local partition error
				this.partition.getStats().errorCount.increment();
				this.partition.writeErrorLog(error);
			}
		}

	}

	private void processRow(final ICsvMapReader partitionReader,
			final Map<String, Object> partitionMap, final String umamId)
			throws Exception {

		printLineHeader(partitionReader, partitionMap, currentUmdmId, umamId,
				processedUmdmId);

		if ((currentUmdmId.equals(processedUmdmId))) {

			umamComponent = createUMAM(partitionMap,
					partitionReader.getLineNumber());

			log.debug("UMAM  has been created = " + umamComponent);

		} else { // create a new UMDM Title whenever umdm_id has been
					// changed to a new value

			if (umdmTitle.getId() != null) {
				uploadTitle(umdmTitle);
			}

			if (umdmTitle.getId() == null) {

				log.debug("First KAP is going to be created from line #: "
						+ partitionReader.getLineNumber());

				log.debug("Current KAP : " + umdmTitle);
			}

			umdmTitle = createKAP(partitionMap, partitionReader.getLineNumber());

			// create first UMAM object

			umamComponent = createUMAM(partitionMap,
					partitionReader.getLineNumber());

			log.debug("UMAM  has been created = " + umamComponent);

		}
		// continue process next row
		processedUmdmId = currentUmdmId;

	}

	private UMAMCSVInput createUMAM(Map<String, Object> row, int lineNumber) {
		UMAMCSVInput umam = null;

		logContentItemCreation(row, lineNumber);

		String umdmId = (String) row.get("umdm_id");
		String umamId = (String) row.get("umam_page_id");
		String umamType = (String) row.get("umam_type");
		String fileName = (String) row.get("filename");
		String filePath = (String) row.get("path");
		String label = (String) row.get("label");
		String rank = (String) row.get("rank");

		umam = new UMAMCSVInput(umdmId, umamId, fileName, label, rank,
				filePath, umamType);

		umam.setLineNumber(lineNumber);
		umam.setPartition(partition);

		umdmTitle.getChildUMAM().put(filePath, umam);

		return umam;
	}

	private void uploadTitle(final UMDMCSVInput umdmTitle) throws Exception {

		if (umdmTitle.getChildUMAM().size() > 0) {

			logTitleUploadingProgress(umdmTitle);
		}

		final Future<UMDMContentObject> umdmTask = KAPService.getUMDMPooll()
				.submit(new KAPUploader(umdmTitle,
						new String[] { "umd:212748" }));

		UMDMContentObject umdmContent = new UMDMContentObject();

		while (true) {
			if (umdmTask.isDone()) {

				log.info("UMDM Content has been loaded." + "UMDM Title : "
						+ umdmTitle.getId() + "; UMDM PID: "
						+ umdmContent.getPid());

				break;
			}
			if (!umdmTask.isDone()) {
				try {
					umdmContent = umdmTask.get();
				} catch (InterruptedException e) {
					log.error("Error uploading item");
				} catch (ExecutionException e) {
					log.error("Error uploading item");
					e.printStackTrace();
				}

				log.info("Waiting for content being loaded. "
						+ "UMDM Title : = " + umdmTitle.getId());
			}

		}

		logTitleUploadingCompletion(umdmContent);

	}

	private void logTitleUploadingCompletion(final UMDMContentObject umdmContent) {

		// log global title count
		LoaderStats.uploadedProcessedUMDMFileCount.increment();

		// log local partition title count
		this.partition.getStats().uploadedProcessedUMDMTitleCount.increment();

		log.info("UMDM Content has been uploaded: PID = "
				+ umdmContent.getPid() + " UMDM Id = " + umdmContent.getId());

		UMDMCSVInput dataLineage = umdmContent.getSourceUMDM();
		log.debug("UMDM DataLineage: UMDM Pid :" + dataLineage.getPid());

		// log global and local UMAM count
		try {
			partition.writeProcessedData(dataLineage);
		} catch (IOException e) {

			String errorMessage = "Error writing input partition UMAM File Metadata. "
					+ partition.getPartitionPath()
					+ ";  "
					+ "Message: "
					+ e.getMessage() + "; Cause: " + e.getCause();

			log.error(errorMessage);
			e.printStackTrace();

			// log global error
			LoaderStats.errorCount.increment();
			ErrorLogEntry error = new ErrorLogEntry(
					LoaderStats.errorCount.getValue(), errorMessage);
			KAPService.writeErrorLog(error);

			// log local partition error
			this.partition.getStats().errorCount.increment();
			this.partition.writeErrorLog(error);
		}

		log.info("----------------------END Uploading------------------------");
	}

	private void logContentItemCreation(Map<String, Object> row, int lineNumber) {

		log.debug("Line # = " + lineNumber + "--- UMAM Processing------");
		log.debug("Adding UMAM objects to the current KAP: "
				+ umdmTitle.getId());

		log.debug("Creating UMAM: " + currentUmdmId + " Page Id = " + umamId
				+ "\n" + "UMAM File:" + (String) row.get("path"));

	}

	private static void logTitleUploadingProgress(final UMDMCSVInput umdmTitle) {
		log.info("----------------------Start Uploading------------------------");
		log.info("Ready to upload to server current KAP : " + umdmTitle);

		log.debug("Current KAP : " + umdmTitle.getId() + " UMAM Count = "
				+ umdmTitle.getChildUMAM().size());

		log.debug("Current UMDM ID : " + currentUmdmId + " Processed UMDM ID: "
				+ processedUmdmId + " UMAM Count = "
				+ umdmTitle.getChildUMAM().size());

		log.info("Processing UMDM Title "
				+ (LoaderStats.uploadedProcessedUMDMFileCount.getValue() + 1)
				+ " of Total UMDM Titles "
				+ LoaderStats.sourceUMDMCount.getValue() + "...");
	}

	private static void logTitleCreation(final UMDMCSVInput umdmTitle,
			int lineNumber) {

		if (umdmTitle.getId() == null) {

			log.debug("First KAP is going to be created from line #: "
					+ lineNumber);
			log.debug("Current KAP : " + umdmTitle);
		}

		log.info("About to generate a new KAP for: " + currentUmdmId);
	}

	public static UMDMCSVInput createKAP(Map<String, Object> row, int lineNumber) {

		logTitleCreation(umdmTitle, lineNumber);

		logTitleUploadingProgress(umdmTitle);

		UMDMCSVInput title = null;

		String umdmId = (String) row.get("umdm_id");
		String collection = (String) row.get("collection");
		String series = (String) row.get("series");
		String subseries = (String) row.get("subseries");
		String box = (String) row.get("box");
		String folder = (String) row.get("folder");
		String itemTitle = (String) row.get("title");
		String item = (String) row.get("item");
		String handle = (String) row.get("handle");
		String accession = (String) row.get("Accession Number");
		String date = (String) row.get("date");
		String size = (String) row.get("size");
		String fileName = (String) row.get("filename");
		String label = (String) row.get("label");
		String rank = (String) row.get("rank");
		String creator = (String) row.get("creator");
		String continent = (String) row.get("continent");
		String country = (String) row.get("country");
		String region = (String) row.get("region");
		String settlement = (String) row.get("settlement");

		title = new UMDMCSVInput(umdmId, lineNumber, "UMDM", collection,
				series, subseries, box, folder, item, itemTitle, date, handle,
				accession, size, fileName, label, rank, creator, continent,
				country, region, settlement);

		title.setLineNumber(lineNumber);
		title.setAccession(accession);

		// report title generation
		log.info("Generated New UMDM Title with UMDM Id = " + currentUmdmId);
		log.info("KAP = " + title);

		return title;
	}

	private void printLineHeader(final ICsvMapReader partitionReader,
			final Map<String, Object> partitionMap, final String currentUmdmId,
			final String umamId, final String processedUmdmId) {
		log.info("Processing line: " + "; Partition path: "
				+ partition.getPartitionPath() + "; Line #: "
				+ partitionReader.getLineNumber());
		log.debug("----------------------------------------------");

		log.debug("Current line content: "
				+ String.format("lineNo=%s, rowNo=%s, kapMap=%s",
						partitionReader.getLineNumber(),
						partitionReader.getRowNumber(), partitionReader));
		log.debug("Current UMDM = " + currentUmdmId + " Processed UMDM ID = "
				+ processedUmdmId);
		log.debug("File Path = " + partitionMap.get("path"));
		log.debug("----------------------------------------------");
	}

	public void setMaxRecordCount(int maxRecordCount) {
		this.maxRecordCount = maxRecordCount;
	}
}
