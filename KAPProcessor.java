package edu.umd.lims.fedora.kap;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.math.NumberUtils;
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

public class KAPProcessor {
/**
	private static final Logger log = LoggerFactory
			.getLogger(KAPProcessor.class);
	private final static StopWatch timer = new StopWatch();
	private static ICsvMapReader kapReader = null;
	static UMDMCSVInput kapMaster = new UMDMCSVInput();
	static UMAMCSVInput umamComponent = null;
	static ICsvMapWriter outputMapWriter = null;

	public static void loadData() throws KAPServiceException {
		timer.reset();
		timer.start();

		log.info("Loading KAP to Fedora has started...");
		log.info("Metadata processed input file: " + KAPService.getMetadataProcessedPath());

		Exception  ex = null;
		
		FileReader fileReader;
		try {
			fileReader = new FileReader(KAPService.getMetadataProcessedPath());
			kapReader = new CsvMapReader(fileReader,
					CsvPreference.STANDARD_PREFERENCE);
			kapReader.getHeader(true);

			final String[] header = DatasetReader.getUMAMFilesHeader();
			final CellProcessor[] processors = DatasetReader
					.getUMAMFilesProcessors();
			Map<String, Object> kapMap;

			String processedUmdmId = null;
			String currentUmdmId = null;
			String umamId = null;
			int currentLineNumber = 0;
			FileWriter processedDataWriter = new FileWriter(
					KAPService.getOutputProcessedPath());

			outputMapWriter = new CsvMapWriter(processedDataWriter,
					CsvPreference.STANDARD_PREFERENCE);

			outputMapWriter.writeHeader(getProcessedUMAMFilesHeader());

			
			// read CSV line by line, compose KAP with UMAM Content, load to
			// Fedora
			while (((kapMap = kapReader.read(header, processors)) != null)
	//				) {
			&& kapReader.getLineNumber()<2) {

				currentLineNumber = kapReader.getLineNumber();
				currentUmdmId = (String) kapMap.get("umdm_id");
				umamId = (String) kapMap.get("umam_page_id");

				try {

					log.info("Processing line #: " + kapReader.getLineNumber());
					log.debug("----------------------------------------------");

					log.debug("Current line content: " + String.format("lineNo=%s, rowNo=%s, kapMap=%s",
							kapReader.getLineNumber(),
							kapReader.getRowNumber(), kapMap));
					log.debug("Current UMDM = " + currentUmdmId
							+ " Processed UMDM ID = " + processedUmdmId);
					log.debug("File Path = " + kapMap.get("path"));
					log.debug("----------------------------------------------");

					if ((currentUmdmId.equals(processedUmdmId))) {

						log.debug("Line # = " + kapReader.getLineNumber()
								+ "--- UMAM Processing------");
						log.debug("Adding UMAM objects to the current KAP: "
								+ kapMaster.getId());

						log.debug("Creating UMAM: " + currentUmdmId
								+ " Page Id = " + umamId + "\n" + "UMAM File:"
								+ (String) kapMap.get("path"));

						umamComponent = createUMAM(kapMap, currentLineNumber);

						log.debug("UMAM  has been created = " + umamComponent);


					} else { // create new KAP object whenever umdm_id has been
								// changed to a new value

						if (kapMaster.getId() == null) {

							log.debug("First KAP is going to be created from line #: "
									+ kapReader.getLineNumber());

							log.debug("Current KAP : " + kapMaster);
						}

						if (kapMaster.getId() != null) {
							log.info("----------------------Start Uploading------------------------");
							log.info("Ready to upload to server current KAP : "
									+ kapMaster);
							int expectedUmamCount = NumberUtils
									.toInt((String) kapMap.get("umam_count"));
							int actualUmamCount = kapMaster.getChildUMAM()
									.size();

							log.debug("Validating KAP integrity : Expected KAP UMAM Count = "
									+ expectedUmamCount
									+ " Actual UMAM Count = " + actualUmamCount);

							log.debug("Current KAP : " + kapMaster.getId()
									+ " UMAM Count = "
									+ kapMaster.getChildUMAM().size());
							
							log.debug("Current UMDM ID : " + currentUmdmId
									+ " Processed UMDM ID: " + processedUmdmId
									+ " UMAM Count = "
									+ kapMaster.getChildUMAM().size());

							KAPUploader kapUpLoader = new KAPUploader(
									kapMaster, new String[] { "umd:5305" });
							
							log.info("Processing KAP Object " + (LoaderStats.uploadedProcessedUMDMFileCount.getValue()+ 1)+ " of Total KAPs " 
									+LoaderStats.sourceUMDMCount.getValue() + "...");

					//		UMDMContentObject umdmContent = kapUpLoader
						//			.upload();

							LoaderStats.uploadedProcessedUMDMFileCount.increment();
							
			//				log.info("UMDM Content has been uploaded: PID = "
		//							+ umdmContent.getPid() + " UMDM Id = "
			//						+ umdmContent.getId());

							UMDMCSVInput dataLineage = umdmContent
									.getSourceUMDM();
							log.debug("UMDM DataLineage: UMDM Pid :"
									+ dataLineage.getPid());

							writeProcessedData(dataLineage);
							log.info("----------------------END Uploading------------------------");
						}

						log.info("About to generate a new KAP for: "
								+ currentUmdmId);

						kapMaster = createKAP(kapMap, currentLineNumber);

						log.info("Generated New KAP Master with UMDM Id = "
								+ currentUmdmId);
						log.info("KAP = " + kapMaster);

						// create first UMAM object
						// log info
						log.debug("Line # = " + kapReader.getLineNumber()
								+ "--- UMAM Processing------");
						log.debug("Adding UMAM objects to the current KAP: "
								+ kapMaster.getId());

						log.info("Generating UMAM: " + currentUmdmId
								+ " Page Id = " + umamId + "\n" + "UMAM File:"
								+ (String) kapMap.get("path"));

						umamComponent = createUMAM(kapMap, currentLineNumber);

						log.debug("UMAM  has been created = " + umamComponent);

					}

					processedUmdmId = currentUmdmId;

				} catch (Exception e) {
					
					// Log error and continue to process next line
					String errorMessage = "Error processing input UMAM File Metadata , line #:"
							+ currentLineNumber + "; " + KAPService.getMetadataProcessedPath() + "; "
							+ "Message: " + e.getMessage() + "; Cause: " + e.getCause();
					
					log.error(errorMessage);
					
					LoaderStats.errorCount.increment();
					ErrorLogEntry error = new ErrorLogEntry(
							LoaderStats.errorCount.getValue(), errorMessage);
					KAPService.writeErrorLog(error);
					
				}

			}
			
			
			// upload the last KAP object
			try {
			if (kapMaster!=null){
				
				log.info("----------------------Start Uploading------------------------");
				log.info("Ready to upload to server the last KAP object : "
						+ kapMaster);
								
				log.info("Current KAP : " + kapMaster.getId()
						+ " UMAM Count = "
						+ kapMaster.getChildUMAM().size());
				
				KAPUploader kapUpLoader = new KAPUploader(
						kapMaster, new String[] { "umd:5305" });
				
				log.info("Processing KAP Object " + (LoaderStats.uploadedProcessedUMDMFileCount.getValue() + 1)+ " of Total KAPs " 
						+LoaderStats.sourceUMDMCount.getValue() + "...");
				
				UMDMContentObject umdmContent = kapUpLoader
						.upload();

				LoaderStats.uploadedProcessedUMDMFileCount.increment();
				
				log.info("UMDM Content has been uploaded: PID = "
						+ umdmContent.getPid() + " UMDM Id = "
						+ umdmContent.getId());

				UMDMCSVInput dataLineage = umdmContent
						.getSourceUMDM();
				log.debug("UMDM DataLineage: UMDM Pid :"
						+ dataLineage.getPid());
				
				writeProcessedData(dataLineage);
				log.info("----------------------END Uploading------------------------");
			}
			} catch(Exception e){
				
				// Log error and continue to process next line
				String errorMessage = "Error processing input UMAM File Metadata , line #:"
						+ currentLineNumber + "; " + KAPService.getMetadataProcessedPath() + "; "
						+ "Message: " + e.getMessage() + "; Cause: " + e.getCause();
				
				log.error(errorMessage);
				
				LoaderStats.errorCount.increment();
				ErrorLogEntry error = new ErrorLogEntry(
						LoaderStats.errorCount.getValue(), errorMessage);
				KAPService.writeErrorLog(error);
				
			}
			

		} catch (FileNotFoundException e) {

			ex = e;
			// Throw error and stop processing
			String errorMessage = "Error processing input UMAM File Metadata. File Not Found Exception" + "; " +
						 KAPService.getMetadataProcessedPath() + "; "
					+ "Message: " + e.getMessage() + "; Cause: " + e.getCause();
			
			log.error(errorMessage);
			
			throw new KAPServiceException(
					"Error reading KAP processed metadata file.",
					ServiceErrorCode.UMAM_MD_ERROR.getValue());
			
		} catch (IOException e) {
			
			ex = e;
			
			String errorMessage = "IO Exception occurred during reading UMAM processed master file." + "; " +
					 KAPService.getMetadataProcessedPath() + "; "
				+ "Message: " + e.getMessage() + "; Cause: " + e.getCause();
			
			log.error(errorMessage);

			throw new KAPServiceException(
					"Error reading KAP processed metadata file.",
					ServiceErrorCode.UMAM_MD_ERROR.getValue());
		} catch (Exception e) {
			
			ex = e; 
			String errorMessage = "Error processing input UMAM File Metadata:" + "; " +
					 KAPService.getMetadataProcessedPath() + "; "
				+ "Message: " + e.getMessage() + "; Cause: " + e.getCause();
			
			log.error(errorMessage);
			
			throw new KAPServiceException(
					"Error reading KAP processed metadata file.",
					ServiceErrorCode.UMAM_MD_ERROR.getValue());

		} finally {

			if (kapReader != null) {
				try {
					kapReader.close();
				} catch (IOException e) {
					log.error("Error when attempt to close Processed Master Data file."
							+ e.getMessage());
				}
			}

				if (outputMapWriter != null) {
				try {
					outputMapWriter.close();
				} catch (IOException e) {
					log.error("Error when attempt to close output processeed data file."
							+ e.getMessage());
				}
			}
				
			if (ex!=null){
				String errorMessage = "Error processing input UMAM File Metadata: "
						+ KAPService.getMetadataProcessedPath() + ";  "
					 + "Message: " + ex.getMessage()
					 + "; Cause: " + ex.getCause();
				
				LoaderStats.errorCount.increment();
				ErrorLogEntry error = new ErrorLogEntry(
						LoaderStats.errorCount.getValue(), errorMessage);
				KAPService.writeErrorLog(error);
			}

		}

		timer.stop();
		log.info("Loading KAP to Fedora Completed. " + " Total time taken. " + timer.toString());

	}

	public static UMDMCSVInput createKAP(Map<String, Object> row, int lineNumber) {
		UMDMCSVInput kap = null;

		String umdmId = (String) row.get("umdm_id");
		String collection = (String) row.get("collection");
		String series = (String) row.get("series");
		String subseries = (String) row.get("subseries");
		String box = (String) row.get("box");
		String folder = (String) row.get("folder");
		String title = (String) row.get("title");
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

		kap = new UMDMCSVInput(umdmId, lineNumber, "UMDM", collection, series,
				subseries, box, folder, item, title, date, handle, accession, size,
				fileName, label, rank, creator, continent, country, region,
				settlement);
		
		kap.setLineNumber(lineNumber);

		return kap;
	}

	public static UMAMCSVInput createUMAM(Map<String, Object> row,
			int lineNumber) {
		UMAMCSVInput umam = null;

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
		
		kapMaster.getChildUMAM().put(filePath, umam);

		return umam;
	}

	public static CellProcessor[] getProcessedUMAMFilesProcessors() {

		final CellProcessor[] processors = new CellProcessor[] { new NotNull(), // UMDM
																				// ID
																				// Type
				new NotNull(), // UMAM Page Id 1
				new Optional(), // Collection 2
				new Optional(), // Series 3
				new Optional(), // SubSeries 4
				new Optional(), // Box 5
				new Optional(), // Folder 6
				new Optional(), // Item 7
				new Optional(), // Title 8
				new Optional(), // Date 9
				new Optional(), // Size 10
				new Optional(), // handle 11
				new NotNull(), // fileName 12
				new NotNull(), // filePath 13
				new NotNull(), // UMAM Type 14
				new NotNull(), // label 15
				new NotNull(), // rank 16
				new Optional(), // creator 17
				new Optional(), // continent 18
				new Optional(), // country 19
				new Optional(), // region 20
				new Optional(), // settlement 21
				new Optional(), // umam_count 22
				new Optional(), // umdm_pid 23
				new Optional(), // umam_pid 24
				new Optional(), // fedora_umdm_url 25
				new Optional(), // fedora_umam_url 26
		};

		return processors;
	}

	public static String[] getProcessedUMAMFilesHeader() {

		final String[] header = new String[] { "umdm_id", "umam_page_id",
				"collection", "series", "subseries", "box", "folder", "item",
				"title", "date", "size", "handle", "filename", "path",
				"umam_type", "label", "rank", "creator", "continent",
				"country", "region", "settlement", "umam_count", "umdm_pid",
				"umam_pid", "fedora_umdm_url", "fedora_umam_url" };

		return header;
	}

	public static String getFedoraURL(String pid) {
		return KAPService.getConnection() + "/get/" + pid;
	}

	public static void writeProcessedData(UMDMCSVInput input)
			throws IOException {

		TreeMap<String, UMAMCSVInput> items = input.getChildUMAM();

		final String[] umamProcessedHeader = getProcessedUMAMFilesHeader();

		log.debug("Writing datalineage results: " + items.size());

		for (Map.Entry<String, UMAMCSVInput> item : items.entrySet()) {

			// add processed Files to the list of UMAM Files
			final Map<String, Object> umamFile = new HashMap<String, Object>();

			UMAMCSVInput umam = item.getValue();

			umamFile.put(umamProcessedHeader[0], input.getId());
			umamFile.put(umamProcessedHeader[1], umam.getId());
			umamFile.put(umamProcessedHeader[2], input.getCollection());
			umamFile.put(umamProcessedHeader[3], input.getSeries());
			umamFile.put(umamProcessedHeader[4], input.getSubseries());
			umamFile.put(umamProcessedHeader[5], input.getBox());
			umamFile.put(umamProcessedHeader[6], input.getFolder());
			umamFile.put(umamProcessedHeader[7], input.getItem());
			umamFile.put(umamProcessedHeader[8], input.getTitle());
			umamFile.put(umamProcessedHeader[9], input.getDate());
			umamFile.put(umamProcessedHeader[10], input.getSize());
			umamFile.put(umamProcessedHeader[11], input.getHandle());
			umamFile.put(umamProcessedHeader[12], umam.getFileName());
			umamFile.put(umamProcessedHeader[13], umam.getFilePath());
			umamFile.put(umamProcessedHeader[14], umam.getType());
			umamFile.put(umamProcessedHeader[15], umam.getLabel());
			umamFile.put(umamProcessedHeader[16], umam.getRank());
			umamFile.put(umamProcessedHeader[17], input.getCreator());
			umamFile.put(umamProcessedHeader[18], input.getContinent());
			umamFile.put(umamProcessedHeader[19], input.getCountry());
			umamFile.put(umamProcessedHeader[20], input.getRegion());
			umamFile.put(umamProcessedHeader[21], input.getSettlement());
			umamFile.put(umamProcessedHeader[22], items.size());
			umamFile.put(umamProcessedHeader[23], input.getPid());
			umamFile.put(umamProcessedHeader[24], umam.getPid());
			umamFile.put(umamProcessedHeader[25], getFedoraURL(input.getPid()));
			umamFile.put(umamProcessedHeader[26], getFedoraURL(umam.getPid()));

								
			// write UMAM files grouped by file path info to the processed file
			// refer to the property "output.processed.path"
			try {
				outputMapWriter.write(umamFile, getProcessedUMAMFilesHeader(),
						getProcessedUMAMFilesProcessors());
				outputMapWriter.flush();
				
				LoaderStats.uploadedProcessedUMAMCount.increment(); 
				
			} catch (IOException e) {
				log.error("Error occurred while writing processed data. "
						+ e.getMessage());
				throw e;
			}

		}

		log.info(LoaderStats.getCurrentStatistics());
	}
	**/
}
