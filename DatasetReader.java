package edu.umd.lims.fedora.kap;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

public class DatasetReader {

	private static final Logger log = LoggerFactory
			.getLogger(DatasetReader.class);

	private final static StopWatch timer = new StopWatch();
	private static UMDMCSVInput umdmInput = null;
	private static UMAMCSVInput umamInput = null;
	private static String parentObjectId = null;
	private static String objectId = null;
	private static String type = null;
	private static String collection = null;
	private static String series = null;
	private static String subseries = null;
	private static String box = null;
	private static String folder = null;
	private static String title = null;
	private static String item = null;
	private static String handle = null;
	private static String accession = null;
	private static String date = null;
	private static String size = null;
	private static String fileName = null;
	private static String label = null;
	private static String rank = null;
	private static String creator = null;
	private static String continent = null;
	private static String country = null;
	private static String region = null;
	private static String settlement = null;
	private static String contentType = null;

	static ICsvMapWriter mapWriter = null;
	static ICsvMapWriter processedMapWriter = null;
	private static Partition filePartition;
	static ICsvMapWriter partitionsStagedSummary = null;

	private static boolean bIsInFilter = false;

	public static void readDataset() throws Exception {

		timer.reset();
		timer.start();

		log.info("Dataset Reading and Preprocessing has started...");

		Exception ex = null;
		ICsvMapReader mapReader = null;

		try {

			int partitionSize = 0;

			if (KAPService.getSampleProbeSize() > 0) { // create sample
														// partition
				partitionSize = KAPService.getSampleProbeSize();
				LoaderStats.partitionCount.increment();
				filePartition = createPartition(
						LoaderStats.partitionCount.getValue(), partitionSize);
			}

			FileReader fileReader = new FileReader(KAPService.getMetadataPath());
			log.info("Reading metadata input file: "
					+ KAPService.getMetadataPath());

			mapReader = new CsvMapReader(fileReader,
					CsvPreference.STANDARD_PREFERENCE);

			mapReader.getHeader(true); // skip past the header

			FileWriter fileWriter = new FileWriter(
					KAPService.getMetadataStagedPath());

			mapWriter = new CsvMapWriter(fileWriter,
					CsvPreference.STANDARD_PREFERENCE);

			final String[] umaamPagesHeader = getUMAMPagesHeader();
			// write the header
			mapWriter.writeHeader(getUMAMPagesHeader());

			FileWriter processedDataWriter = new FileWriter(
					KAPService.getMetadataProcessedPath());

			processedMapWriter = new CsvMapWriter(processedDataWriter,
					CsvPreference.STANDARD_PREFERENCE);
			processedMapWriter.writeHeader(getUMAMFilesHeader());

			FileWriter partitionsSummaryWriter = new FileWriter(
					KAPService.getStagedPartitionsPath()
							+ "/partitions-summary.csv");

			partitionsStagedSummary = new CsvMapWriter(partitionsSummaryWriter,
					CsvPreference.STANDARD_PREFERENCE);
			partitionsStagedSummary.writeHeader(getPartitionsSummaryHeader());

			final String[] header = getHeader();
			final CellProcessor[] processors = getProcessors();

			final String[] umamFilesHeader = getUMAMFilesHeader();

			Map<String, Object> kapMap;

			parentObjectId = null;
			objectId = null;
			type = null;
			collection = null;
			series = null;
			subseries = null;
			box = null;
			folder = null;
			title = null;
			item = null;
			handle = null;
			accession = null;
			date = null;
			size = null;
			fileName = null;
			label = null;
			rank = null;
			creator = null;
			continent = null;
			country = null;
			region = null;
			settlement = null;

			umdmInput = null;
			umamInput = null;

			partitionSize = KAPService.getBatchSize();

			if (filePartition == null) { // validate whether initial partition
											// exists
				LoaderStats.partitionCount.increment();
				filePartition = createPartition(
						LoaderStats.partitionCount.getValue(), partitionSize);
			}

			while ((kapMap = mapReader.read(header, processors)) != null) {

				LoaderStats.sourceCount.increment();

				int currentLineNumber = mapReader.getLineNumber();
				contentType = null;
				fileName = null;

				Exception exception = null;
				umamInput = null;

				try {

					log.debug("Reading/processing metadata line #: "
							+ mapReader.getLineNumber()
							+ " "
							+ String.format("lineNo=%s, rowNo=%s, kapMap=%s",
									mapReader.getLineNumber(),
									mapReader.getRowNumber(), kapMap));

					Map<String, Object> processedRow = preProcessRow(kapMap);

					// Part of the createUMAMPage routine is to test to see if the current row 
					// belongs in the filter we may have established.
					// It sets bIsInFilter to true or false if the processedRow is a 
					// UMDM row rather than a UMAM row.
					createUMAMPage(processedRow, mapReader.getLineNumber(),
							umaamPagesHeader, umamFilesHeader);

					if (bIsInFilter) {
						String contentType = (String) processedRow
								.get("XML Type");
						if (!filePartition.canAdd()
								&& contentType.equals(ContentType.UMDM
										.toString())) {

							writePartitionsSummary(filePartition);

							if (filePartition.getPartitionWriter() != null) {
								filePartition.getPartitionWriter().flush();
								filePartition.getPartitionWriter().close();
							}

							LoaderStats.partitionCount.increment();
							filePartition = createPartition(
									LoaderStats.partitionCount.getValue(),
									partitionSize);
						}
						writePartitionRow(processedRow,
								mapReader.getLineNumber());
					}

				} catch (Exception e) { // skip processing of the current line
										// and report the error

					exception = e;

					e.printStackTrace();

					String errorMessage = "Error processing input metadata file: "
							+ KAPService.getMetadataPath()
							+ ";  "
							+ "; Line #: "
							+ +currentLineNumber
							+ "; Message: "
							+ e.getMessage() + "; Cause: " + e.getCause();

					log.error(errorMessage);

					LoaderStats.errorCount.increment();

					ErrorLogEntry error = new ErrorLogEntry(
							LoaderStats.errorCount.getValue(), errorMessage);
					KAPService.writeErrorLog(error);

				}
			}
		} catch (Exception e) {

			log.error("Error occurred while reading input metadata dataset. "
					+ KAPService.getMetadataPath() + "; " + e.getMessage());
			ex = e;

			// throw error, and stop processing

			e.printStackTrace();
			throw new KAPServiceException(e.getMessage(),
					ServiceErrorCode.DSRD_ERROR.getValue());

		} finally {

			if (ex == null) {

				log.info("Total Metadata Record Count: "
						+ LoaderStats.sourceCount.getValue()
						+ ". Metadata has been read from "
						+ KAPService.getMetadataPath());

				log.info("Total Source UMAM Pages Count: "
						+ LoaderStats.stagedUMAMPagesCount.getValue()
						+ ". UMAM Pages metadata has been written to output file : "
						+ KAPService.getMetadataStagedPath());

				log.info("Total Preprocessed UMAM Files Count: "
						+ LoaderStats.masterProcessedUMAMFileCount.getValue()
						+ ". Preprocessed UMAM Files metadata has been written to output file : "
						+ KAPService.getMetadataProcessedPath());

				writePartitionsSummary(filePartition);

			} else {

				ex.printStackTrace();
				log.error("Cannot identify retrieved Metadata Record Count. Error occurred while reading input metadata dataset. "
						+ KAPService.getMetadataPath() + " " + ex.getMessage());

				LoaderStats.errorCount.increment();

				String errorMessage = "Error processing input metadata file: "
						+ KAPService.getMetadataPath() + ";  " + "Message: "
						+ ex.getMessage() + "; Cause: " + ex.getCause();

				ErrorLogEntry error = new ErrorLogEntry(
						LoaderStats.errorCount.getValue(), errorMessage);
				KAPService.writeErrorLog(error);

			}

			if (mapReader != null) {
				mapReader.close();
			}

			if (mapWriter != null) {
				mapWriter.close();
			}

			if (processedMapWriter != null) {
				processedMapWriter.close();
			}

			if (partitionsStagedSummary != null) {
				partitionsStagedSummary.close();
			}

		}

		timer.stop();

		log.info("Dataset Reading and Preprocessing has been completed. "
				+ " Total time taken. " + timer.toString());
	}

	private static CellProcessor[] getProcessors() {

		// apply some constraints to ignored columns (just because we can)
		final CellProcessor[] processors = new CellProcessor[] { new NotNull(), // XML
																				// Type
				new Optional(), // Collection
				new Optional(), // Series
				new Optional(), // SubSeries
				new Optional(), // Box
				new Optional(), // Folder
				new Optional(), // Item
				new Optional(), // Title
				new Optional(), // Date
				new Optional(), // Size
				null, // Restricted
				new Optional(), // handle
				new Optional(), // Accession
				null, // Handwritten or typed?
				null, // handwritten or typed controlled
				null, // Onionskin?
				null, // Column
				new NotNull(), // fileName
				new NotNull(), // label
				new NotNull(), // rank
				new Optional(), // creator
				new Optional(), // continent
				new Optional(), // country
				new Optional(), // region
				new Optional(), // settlement
				null, // umdm pid
				null, // umam pid
				null, // TEI
		};

		return processors;
	}

	private static CellProcessor[] getUMAMPagesProcessors() {

		// apply some constraints to ignored columns (just because we can)
		final CellProcessor[] processors = new CellProcessor[] { new NotNull(), // UMDM
																				// ID
																				// Type
				new NotNull(), // UMAM Page Id
				new Optional(), // Collection
				new Optional(), // Series
				new Optional(), // SubSeries
				new Optional(), // Box
				new Optional(), // Folder
				new Optional(), // Item
				new Optional(), // Title
				new Optional(), // Date
				new Optional(), // Size
				new Optional(), // handle
				new Optional(), // Accession
				new NotNull(), // fileName
				new NotNull(), // label
				new NotNull(), // rank
				new Optional(), // creator
				new Optional(), // continent
				new Optional(), // country
				new Optional(), // region
				new Optional(), // settlement
		};

		return processors;
	}

	private static String[] getHeader() {

		final String[] header = new String[] { "XML Type", "collection",
				"series", "subseries", "box", "folder", "item", "title",
				"date", "size", null, "handle", "Accession Number", null, null,
				null, null, "filename", "label", "rank", "creator",
				"continent", "country", "region", "settlement", null, null,
				null };

		return header;
	}

	private static String[] getUMAMPagesHeader() {

		final String[] header = new String[] { "umdm_id", "umam_page_id",
				"collection", "series", "subseries", "box", "folder", "item",
				"title", "date", "size", "handle", "Accession Number",
				"filename", "label", "rank", "creator", "continent", "country",
				"region", "settlement" };

		return header;
	}

	private static String getObjectId(String contentType, String fileName) {

		String result = null;

		if (contentType.equals(ContentType.UMAM.toString())) {

			result = fileName.trim();

		} else if (contentType.equals(ContentType.UMDM.toString())) {
			int index = fileName.lastIndexOf("-");
			result = fileName.substring(0, index);

		}

		return result;

	}

	private static String getParentObjectId(String fileName) {

		String result = null;
		int index = fileName.lastIndexOf("-");
		result = fileName.substring(0, index);

		return result;

	}

	private static String trimField(String field) {

		String result = null;

		if ((field != null) || (!field.isEmpty())) {
			result = field.trim();
		}

		return result;
	}

	private static Map<String, Object> preProcessRow(Map<String, Object> map) {

		Map<String, Object> items = map;

		for (Entry<String, Object> item : items.entrySet()) {
			String key = item.getKey();
			String value = (String) item.getValue();

			if (value != null) {
				value.trim();
				items.put(key, value);
			}
		}

		return items;
	}

	public static void createUMAMPage(Map<String, Object> row, int lineNumber,
			String[] umamPagesHeader, String[] umamFilesHeader) {

		Exception ex = null;

		try {
			contentType = (String) row.get("XML Type");
			fileName = (String) row.get("filename");

			objectId = getObjectId(contentType, fileName);
			parentObjectId = getParentObjectId(fileName);

			log.debug("Metadata line #: " + lineNumber
					+ " Found Content type: " + contentType + "."
					+ " Parent Object Id: " + parentObjectId + "."
					+ " Object Id: " + objectId);

			type = (String) row.get("XML Type");
			collection = (String) row.get("collection");
			series = (String) row.get("series");
			subseries = (String) row.get("subseries");
			box = (String) row.get("box");
			folder = (String) row.get("folder");
			title = (String) row.get("title");
			item = (String) row.get("item");
			handle = (String) row.get("handle");
			accession = (String) row.get("Accession Number");
			date = (String) row.get("date");
			size = (String) row.get("size");
			fileName = (String) row.get("filename");
			label = (String) row.get("label");
			rank = (String) row.get("rank");
			creator = (String) row.get("creator");
			continent = (String) row.get("continent");
			country = (String) row.get("country");
			region = (String) row.get("region");
			settlement = (String) row.get("settlement");

			if (contentType.equals(ContentType.UMDM.toString())) {

				// So first we run the partition folder filter
				// to see if this UMDM is worthy of consideration
				String sPartitionFilter = KAPService.getPartitionFolder();
				
				log.info("Partition Folder: " + sPartitionFilter);

				if (sPartitionFilter == null || sPartitionFilter.length() < 1) {
					// The filter is not set and ALL UMDMs get processed
					bIsInFilter = true;
				} else {
					// The filter has been set and we need to see if the JPEG
					// under this UMDM is beneath the folder designated.
					Path rootPath = Paths.get(sPartitionFilter);

					File root = rootPath.toFile();

					TreeMap<String, String> allFiles = new TreeMap<String, String>();

					log.debug("Searching for page content by pattern : "
							+ objectId);

					FileProcessor.searchForUMAMFiles(root, allFiles,
							parentObjectId);

					if (allFiles.size() > 0) {
						bIsInFilter = true;
					} else {
						log.info("Partition Folder has no files for this record");
						bIsInFilter = false;
					}
				}

				if (bIsInFilter) {
					umdmInput = new UMDMCSVInput(objectId, lineNumber, type,
							collection, series, subseries, box, folder, item,
							title, date, handle, accession, size, fileName,
							label, rank, creator, continent, country, region,
							settlement);
				}

			} else if (contentType.equals(ContentType.UMAM.toString())
					&& bIsInFilter) {
				umamInput = new UMAMCSVInput(parentObjectId, objectId,
						fileName, label, rank);

				// add UMAM page

				log.debug("Creating UMAM Page, metadata line #: " + lineNumber
						+ "." + " Page : " + objectId);

				final Map<String, Object> umamPage = new HashMap<String, Object>();

				umamPage.put(umamPagesHeader[0], parentObjectId);
				umamPage.put(umamPagesHeader[1], objectId);
				umamPage.put(umamPagesHeader[2], collection);
				umamPage.put(umamPagesHeader[3], series);
				umamPage.put(umamPagesHeader[4], subseries);
				umamPage.put(umamPagesHeader[5], box);
				umamPage.put(umamPagesHeader[6], folder);
				umamPage.put(umamPagesHeader[7], item);
				umamPage.put(umamPagesHeader[8], title);
				umamPage.put(umamPagesHeader[9], date);
				umamPage.put(umamPagesHeader[10], size);
				umamPage.put(umamPagesHeader[11], handle);
				umamPage.put(umamPagesHeader[12], accession);
				umamPage.put(umamPagesHeader[13], fileName);
				umamPage.put(umamPagesHeader[14], label);
				umamPage.put(umamPagesHeader[15], rank);
				umamPage.put(umamPagesHeader[16], creator);
				umamPage.put(umamPagesHeader[17], continent);
				umamPage.put(umamPagesHeader[18], country);
				umamPage.put(umamPagesHeader[19], region);
				umamPage.put(umamPagesHeader[20], settlement);

				// write UMAM pages info to staged file
				// refer to the property "masterStagedDataPath"
				mapWriter.write(umamPage, umamPagesHeader,
						getUMAMPagesProcessors());

				mapWriter.flush();

				log.debug("UMAM Page has been created metadata line #: "
						+ lineNumber + "." + " Page : " + objectId);

				Path rootPath = Paths.get(KAPService.getDataPath());

				File root = rootPath.toFile();

				TreeMap<String, String> files = new TreeMap<String, String>();
				TreeMap<String, String> allFiles = new TreeMap<String, String>();

				log.debug("Searching for page content by pattern : " + objectId);

				FileProcessor.searchForUMAMFiles(root, files, objectId);

				FileProcessor
						.searchForUMAMFiles(root, allFiles, parentObjectId);

				int umamCount = allFiles.size();

				log.debug("Searching for all content for UMDM Title : "
						+ parentObjectId + ". "
						+ "UMDM Title Total UMAM Count = " + umamCount);

				for (Entry<String, String> file : files.entrySet()) {

					String filePath = file.getValue().toString();
					String fileExtension = FilenameUtils.getExtension(filePath);
					String umamType = "";

					if (fileExtension.equals("jpg")) {
						umamType = "jpg";
					} else if (fileExtension.equals("txt")) {
						umamType = "ocr";
					} else if (fileExtension.equals("xml")) {
						umamType = "hocr";
					} else if (fileExtension.equals("tiff")) {
						umamType = "tiff";
					} else if (fileExtension.equals("pdf")) {
						umamType = "pdf";
					}

					log.debug("Found Content item. File path: " + filePath
							+ "." + " UMAM Type: " + umamType);

					if (filePartition.canAdd()) { // write current file to
													// partition

					}

					// add processed Files to the list of UMAM Files
					final Map<String, Object> umamFile = new HashMap<String, Object>();

					umamFile.put(umamFilesHeader[0], parentObjectId);
					umamFile.put(umamFilesHeader[1], objectId);
					umamFile.put(umamFilesHeader[2], umdmInput.getCollection());
					umamFile.put(umamFilesHeader[3], umdmInput.getSeries());
					umamFile.put(umamFilesHeader[4], umdmInput.getSubseries());
					umamFile.put(umamFilesHeader[5], umdmInput.getBox());
					umamFile.put(umamFilesHeader[6], umdmInput.getFolder());
					umamFile.put(umamFilesHeader[7], umdmInput.getItem());
					umamFile.put(umamFilesHeader[8], umdmInput.getTitle());
					umamFile.put(umamFilesHeader[9], umdmInput.getDate());
					umamFile.put(umamFilesHeader[10], umdmInput.getSize());
					umamFile.put(umamFilesHeader[11], umdmInput.getHandle());
					umamFile.put(umamFilesHeader[12], umdmInput.getAccession());
					umamFile.put(umamFilesHeader[13], fileName);
					umamFile.put(umamFilesHeader[14], filePath);
					umamFile.put(umamFilesHeader[15], umamType);
					umamFile.put(umamFilesHeader[16], label);
					umamFile.put(umamFilesHeader[17], rank);
					umamFile.put(umamFilesHeader[18], umdmInput.getCreator());
					umamFile.put(umamFilesHeader[19], umdmInput.getContinent());
					umamFile.put(umamFilesHeader[20], umdmInput.getCountry());
					umamFile.put(umamFilesHeader[21], umdmInput.getRegion());
					umamFile.put(umamFilesHeader[22], umdmInput.getSettlement());
					umamFile.put(umamFilesHeader[23], allFiles.size());

					// write UMAM files grouped by filePath info to processed
					// file
					// refer to the property "processedMasterData"
					processedMapWriter.write(umamFile, umamFilesHeader,
							getUMAMFilesProcessors());

					processedMapWriter.flush();

					LoaderStats.masterProcessedUMAMFileCount.increment();
					LoaderStats.masterProcessedCount.increment();

				}

			}

		} catch (Exception e) {
			ex = e;

			e.printStackTrace();
			// Log error and continue to process next line
			log.error("Error processing input metadata file, line #:"
					+ lineNumber + "; " + KAPService.getMetadataPath() + "; "
					+ "; Message: " + ex.getMessage() + "Cause: "
					+ ex.getCause());

		} finally {

			if (ex == null) {

				contentType = (String) row.get("XML Type");

				if (contentType.equals(ContentType.UMDM.toString())) {
					LoaderStats.sourceUMDMCount.increment();
				} else if (contentType.equals(ContentType.UMAM.toString())) {

					LoaderStats.sourceUMAMPagesCount.increment();
					LoaderStats.stagedUMAMPagesCount.increment();

				}
			} else {

				ex.printStackTrace();

				LoaderStats.errorCount.increment();
				String errorMessage = "Error processing input metadata file: "
						+ KAPService.getMetadataPath() + "; Line #: "
						+ lineNumber + "; " + "; Message: " + ex.getMessage()
						+ "Cause: " + ex.getCause();
				ErrorLogEntry error = new ErrorLogEntry(
						LoaderStats.errorCount.getValue(), errorMessage);
				KAPService.writeErrorLog(error);

			}

		}

	}

	public static String[] getUMAMFilesHeader() {

		final String[] header = new String[] { "umdm_id", "umam_page_id",
				"collection", "series", "subseries", "box", "folder", "item",
				"title", "date", "size", "handle", "Accession Number",
				"filename", "path", "umam_type", "label", "rank", "creator",
				"continent", "country", "region", "settlement", "umam_count" };

		return header;
	}

	public static CellProcessor[] getUMAMFilesProcessors() {

		final CellProcessor[] processors = new CellProcessor[] { new NotNull(), // UMDM
																				// ID
																				// Type
				new NotNull(), // UMAM Page Id
				new Optional(), // Collection
				new Optional(), // Series
				new Optional(), // SubSeries
				new Optional(), // Box
				new Optional(), // Folder
				new Optional(), // Item
				new Optional(), // Title
				new Optional(), // Date
				new Optional(), // Size
				new Optional(), // handle
				new Optional(), // accession number
				new NotNull(), // fileName
				new NotNull(), // filePath
				new NotNull(), // UMAM Type
				new NotNull(), // label
				new NotNull(), // rank
				new Optional(), // creator
				new Optional(), // continent
				new Optional(), // country
				new Optional(), // region
				new Optional(), // settlement
				new Optional(), // umam_count
		};

		return processors;
	}

	public static String[] getPartitionsSummaryHeader() {

		final String[] header = new String[] { "part_id", "path", "umdm_count",
				"umam_count", "actual_umdm_count", "actual_umam_count",
				"error_count" };

		return header;
	}

	public static CellProcessor[] getPartitionsSummaryProcessors() {

		final CellProcessor[] processors = new CellProcessor[] { new NotNull(), // partition
																				// #
				new NotNull(), // file path
				new Optional(), // umdm_count
				new Optional(), // umam_count
				new Optional(), // actual_umdm_count
				new Optional(), // actual_umam_count
				new Optional(), // error_count
		};

		return processors;
	}

	private static Partition createPartition(int partitionNumber,
			int maxRecordCount) throws IOException {

		Partition partition = new Partition(
				LoaderStats.partitionCount.getValue(), maxRecordCount);
		partition.getPartitionWriter().writeHeader(
				Partition.getPartitionStagedFilesHeader());

		return partition;
	}

	private static void writePartitionsSummary(Partition partition) {

		String[] header = getPartitionsSummaryHeader();

		// add created partition to summary staged file
		final Map<String, Object> currentPartition = new HashMap<String, Object>();

		currentPartition.put(header[0], partition.getPartitionNumber());
		currentPartition.put(header[1], partition.getPartitionPath());
		currentPartition.put(header[2],
				partition.getStats().stagedUMDMTitleCount.getValue());
		currentPartition.put(header[3],
				partition.getStats().stagedUMAMFileCount.getValue());

		try {
			partitionsStagedSummary.write(currentPartition, header,
					getPartitionsSummaryProcessors());
			partitionsStagedSummary.flush();
		} catch (IOException e) {
			log.error("Cannot write staged partitions control information.");
		}

	}

	private static void writePartitionRow(Map<String, Object> row,
			int lineNumber) {
		Exception ex = null;

		String[] header = Partition.getPartitionStagedFilesHeader();

		try {
			contentType = (String) row.get("XML Type");
			fileName = (String) row.get("filename");

			objectId = getObjectId(contentType, fileName);
			parentObjectId = getParentObjectId(fileName);

			log.debug("Metadata line #: " + lineNumber
					+ " Found Content type: " + contentType + "."
					+ " Parent Object Id: " + parentObjectId + "."
					+ " Object Id: " + objectId);

			type = (String) row.get("XML Type");
			collection = (String) row.get("collection");
			series = (String) row.get("series");
			subseries = (String) row.get("subseries");
			box = (String) row.get("box");
			folder = (String) row.get("folder");
			title = (String) row.get("title");
			item = (String) row.get("item");
			handle = (String) row.get("handle");
			accession = (String) row.get("Accession Number");
			date = (String) row.get("date");
			size = (String) row.get("size");
			fileName = (String) row.get("filename");
			label = (String) row.get("label");
			rank = (String) row.get("rank");
			creator = (String) row.get("creator");
			continent = (String) row.get("continent");
			country = (String) row.get("country");
			region = (String) row.get("region");
			settlement = (String) row.get("settlement");

			if (contentType.equals(ContentType.UMDM.toString())) {

				umdmInput = new UMDMCSVInput(objectId, lineNumber, type,
						collection, series, subseries, box, folder, item,
						title, date, handle, accession, size, fileName, label,
						rank, creator, continent, country, region, settlement);

				if (filePartition.canAdd()) {
					filePartition.getStats().stagedUMDMTitleCount.increment();
				}

			} else if (contentType.equals(ContentType.UMAM.toString())) {
				umamInput = new UMAMCSVInput(parentObjectId, objectId,
						fileName, label, rank);

				Path rootPath = Paths.get(KAPService.getDataPath());

				File root = rootPath.toFile();

				TreeMap<String, String> files = new TreeMap<String, String>();
				TreeMap<String, String> allFiles = new TreeMap<String, String>();

				log.debug("Searching for page content by pattern : " + objectId);

				FileProcessor.searchForUMAMFiles(root, files, objectId);

				FileProcessor
						.searchForUMAMFiles(root, allFiles, parentObjectId);

				int umamCount = allFiles.size();

				log.debug("Searching for all content for UMDM Title : "
						+ parentObjectId + ". "
						+ "UMDM Title Total UMAM Count = " + umamCount);

				for (Entry<String, String> file : files.entrySet()) {

					String filePath = file.getValue().toString();
					String fileExtension = FilenameUtils.getExtension(filePath);
					String umamType = "";

					if (fileExtension.equals("jpg")) {
						umamType = "jpg";
					} else if (fileExtension.equals("txt")) {
						umamType = "ocr";
					} else if (fileExtension.equals("xml")) {
						umamType = "hocr";
					} else if (fileExtension.equals("tiff")) {
						umamType = "tiff";
					} else if (fileExtension.equals("pdf")) {
						umamType = "pdf";
					}

					log.debug("Found Content item. File path: " + filePath
							+ "." + " UMAM Type: " + umamType);

					if (filePartition.canAdd()) { // write current file to
													// partition

					}

					// add processed Files to the list of UMAM Files
					final Map<String, Object> umamFile = new HashMap<String, Object>();

					umamFile.put(header[0], parentObjectId);
					umamFile.put(header[1], objectId);
					umamFile.put(header[2], umdmInput.getCollection());
					umamFile.put(header[3], umdmInput.getSeries());
					umamFile.put(header[4], umdmInput.getSubseries());
					umamFile.put(header[5], umdmInput.getBox());
					umamFile.put(header[6], umdmInput.getFolder());
					umamFile.put(header[7], umdmInput.getItem());
					umamFile.put(header[8], umdmInput.getTitle());
					umamFile.put(header[9], umdmInput.getDate());
					umamFile.put(header[10], umdmInput.getSize());
					umamFile.put(header[11], umdmInput.getHandle());
					umamFile.put(header[12], umdmInput.getAccession());
					umamFile.put(header[13], fileName);
					umamFile.put(header[14], filePath);
					umamFile.put(header[15], umamType);
					umamFile.put(header[16], label);
					umamFile.put(header[17], rank);
					umamFile.put(header[18], umdmInput.getCreator());
					umamFile.put(header[19], umdmInput.getContinent());
					umamFile.put(header[20], umdmInput.getCountry());
					umamFile.put(header[21], umdmInput.getRegion());
					umamFile.put(header[22], umdmInput.getSettlement());
					umamFile.put(header[23], allFiles.size());

					// write UMAM files grouped by filePath info to processed
					// file
					// refer to the property "processedMasterData"
					filePartition.getPartitionWriter().write(umamFile, header,
							Partition.getPartitionStagedFilesProcessors());

					filePartition.getPartitionWriter().flush();

					filePartition.getStats().stagedUMAMFileCount.increment();

				}

			}

		} catch (Exception e) {
			ex = e;

			e.printStackTrace();

			// Log error and continue to process next line
			log.error("Error processing input metadata file, line #:"
					+ lineNumber + "; " + KAPService.getMetadataPath() + "; "
					+ "; Message: " + ex.getMessage() + "Cause: "
					+ ex.getCause());

		} finally {

			if (ex == null) {

			} else {

			}

		}

	}
}
