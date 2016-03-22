package edu.umd.lims.fedora.kap;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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

public class Partition {

	private static final Logger log = LoggerFactory.getLogger(Partition.class);

	private ICsvMapWriter partitionWriter;
	private ICsvMapWriter partitionProcessedWriter;
	private ICsvMapWriter partitionErrorWriter;

	private ICsvMapReader partitionReader;
	private String partitionPath;
	private String partitionProcessedPath;
	private String partitionLogErrorPath;
	private int partitionNumber;
	private int maxTitleCount;

	private PartitionStats stats = new PartitionStats();

	Partition(int partitionNumber, int maxTitleCount) throws IOException {
		this.partitionNumber = partitionNumber;
		this.partitionPath = KAPService.getStagedPartitionsPath()
				+ "/part-staged" + String.format("%04d", this.partitionNumber)
				+ ".csv";
		this.partitionProcessedPath = KAPService.getProcessedPartitionsPath()
				+ "/part-processed"
				+ String.format("%04d", this.partitionNumber) + ".csv";
		this.partitionLogErrorPath = KAPService
				.getErrorsLogPath()
				+ "/part-errors"
				+ String.format("%04d", this.partitionNumber) + ".csv";
		this.maxTitleCount = maxTitleCount;
		init();
	}

	Partition(int partitionNumber, String partitionPath, int umdmCount,
			int umamCount) throws IOException {
		this.partitionNumber = partitionNumber;

		this.partitionPath = partitionPath;

		this.partitionProcessedPath = KAPService.getProcessedPartitionsPath()
				+ "/part-processed"
				+ String.format("%04d", this.partitionNumber) + ".csv";
		this.partitionLogErrorPath = KAPService
				.getErrorsLogPath()
				+ "/part-errors"
				+ String.format("%04d", this.partitionNumber) + ".csv";

		this.stats.stagedUMDMTitleCount.setValue(umdmCount);
		this.stats.stagedUMAMFileCount.setValue(umamCount);

		initProcess();
	}

	Partition(int partitionNumber, String partitionPath) throws IOException {
		this.partitionNumber = partitionNumber;

		this.partitionPath = partitionPath;

		this.partitionProcessedPath = KAPService.getProcessedPartitionsPath()
				+ "/part-processed"
				+ String.format("%04d", this.partitionNumber) + ".csv";
		this.partitionLogErrorPath = KAPService
				.getErrorsLogPath()
				+ "/part-errors"
				+ String.format("%04d", this.partitionNumber) + ".csv";

		initReadonly();
	}
	
	private void initReadonly() throws IOException {

	
		FileReader partitionReader = new FileReader(this.partitionPath);

		this.partitionReader = new CsvMapReader(partitionReader,
				CsvPreference.STANDARD_PREFERENCE);
		
		this.partitionReader.getHeader(true);

	}
	
	private void initProcess() throws IOException {

		FileWriter partitionProcessedWriter = new FileWriter(
				this.partitionProcessedPath);

		FileWriter partitionErrorWriter = new FileWriter(
				this.partitionLogErrorPath);

		FileReader partitionReader = new FileReader(this.partitionPath);

		this.partitionProcessedWriter = new CsvMapWriter(
				partitionProcessedWriter, CsvPreference.STANDARD_PREFERENCE);

		this.partitionErrorWriter = new CsvMapWriter(partitionErrorWriter,
				CsvPreference.STANDARD_PREFERENCE);

		this.partitionReader = new CsvMapReader(partitionReader,
				CsvPreference.STANDARD_PREFERENCE);
		
		this.partitionReader.getHeader(true);

	}

	private void init() throws IOException {

		FileWriter partitionWriter = new FileWriter(this.partitionPath);
		FileWriter partitionProcessedWriter = new FileWriter(
				this.partitionProcessedPath);
		FileWriter partitionErrorWriter = new FileWriter(
				this.partitionLogErrorPath);
		FileReader partitionReader = new FileReader(this.partitionPath);

		this.partitionWriter = new CsvMapWriter(partitionWriter,
				CsvPreference.STANDARD_PREFERENCE);

		this.partitionProcessedWriter = new CsvMapWriter(
				partitionProcessedWriter, CsvPreference.STANDARD_PREFERENCE);

		this.partitionErrorWriter = new CsvMapWriter(partitionErrorWriter,
				CsvPreference.STANDARD_PREFERENCE);

		this.partitionReader = new CsvMapReader(partitionReader,
				CsvPreference.STANDARD_PREFERENCE);

	}

	public int getPartitionNumber() {
		return this.partitionNumber;
	}

	public ICsvMapWriter getPartitionWriter() {
		return this.partitionWriter;
	}

	public ICsvMapWriter getProcessedWriter() {
		return this.partitionProcessedWriter;
	}

	public ICsvMapWriter getProcessedErrorWriter() {
		return this.partitionErrorWriter;
	}

	public ICsvMapReader getPartitionReader() {
		return this.partitionReader;
	}

	public String getPartitionPath() {
		return this.partitionPath;
	}

	public String getPartitionProcessedPath() {
		return this.partitionProcessedPath;
	}

	public String getPartitionLogErrorPath() {
		return this.partitionLogErrorPath;
	}

	public boolean canAdd() {
		boolean result = false;

		if (stats.stagedUMDMTitleCount.getValue() < this.maxTitleCount) {
			result = true;
		}
		return result;
	}
	
	public boolean canAddUMDM() {
		boolean result = false;

		if (stats.stagedUMDMTitleCount.getValue() <= this.maxTitleCount) {
			result = true;
		}
		return result;
	}

	public String toString() {
		return "Partition path: " + getPartitionPath() + " : "
				+ "Partition # :" + getPartitionNumber()
				+ " UMDM Title Count = " + stats.stagedUMDMTitleCount.getValue()
				+ "; " + " UMAM Title Count = "
				+ stats.stagedUMAMFileCount.getValue() + "; "
				+ " Actual UMDM Title Count = "
				+ stats.uploadedProcessedUMDMTitleCount.getValue() + "; "
				+ " Actual UMAM File Count = "
				+ stats.uploadedProcessedUMAMFileCount.getValue() + "; ";

	}

	public PartitionStats getStats() {
		return this.stats;
	}

	
    public void writePartitionHeaders() throws IOException{
		this.partitionProcessedWriter.writeHeader(getPartitionProcessedFilesHeader());
		this.partitionErrorWriter.writeHeader(Partition.getErrorLogHeader());
		
		this.partitionProcessedWriter.flush();
		this.partitionErrorWriter.flush();
		
	}

    public  void writeErrorLog(ErrorLogEntry error) {

		final Map<String, Object> logEntry = new HashMap<String, Object>();
		logEntry.put(Partition.getErrorLogHeader()[0], error.getErrorId());
		logEntry.put(Partition.getErrorLogHeader()[1], error.getMessage());
		logEntry.put(Partition.getErrorLogHeader()[2],
				DateUtils.getFormattedCurrentTime());

		try {
			
			this.partitionErrorWriter.write(logEntry, Partition.getErrorLogHeader(),
					getLogErrorProcessors());
			
			this.partitionErrorWriter.flush();

		} catch (IOException e) {
			log.error("Cannot write to error log writer by the following path: "
					+ this.partitionLogErrorPath);
		}

	}
    
    public void writeProcessedData(UMDMCSVInput input)
			throws IOException {

		TreeMap<String, UMAMCSVInput> items = input.getChildUMAM();

		final String[] partitionProcessedHeader = Partition.getPartitionProcessedFilesHeader();

		log.debug("Writing datalineage results: " + items.size());

		for (Map.Entry<String, UMAMCSVInput> item : items.entrySet()) {

			// add processed Files to the list of UMAM Files
			final Map<String, Object> umamFile = new HashMap<String, Object>();

			UMAMCSVInput umam = item.getValue();

			umamFile.put(partitionProcessedHeader[0], input.getId());
			umamFile.put(partitionProcessedHeader[1], umam.getId());
			umamFile.put(partitionProcessedHeader[2], input.getCollection());
			umamFile.put(partitionProcessedHeader[3], input.getSeries());
			umamFile.put(partitionProcessedHeader[4], input.getSubseries());
			umamFile.put(partitionProcessedHeader[5], input.getBox());
			umamFile.put(partitionProcessedHeader[6], input.getFolder());
			umamFile.put(partitionProcessedHeader[7], input.getItem());
			umamFile.put(partitionProcessedHeader[8], input.getTitle());
			umamFile.put(partitionProcessedHeader[9], input.getDate());
			umamFile.put(partitionProcessedHeader[10], input.getSize());
			umamFile.put(partitionProcessedHeader[11], input.getHandle());
			
			umamFile.put(partitionProcessedHeader[12], input.getAccession()); //accession number
			
			umamFile.put(partitionProcessedHeader[13], umam.getFileName());
			umamFile.put(partitionProcessedHeader[14], umam.getFilePath());
			umamFile.put(partitionProcessedHeader[15], umam.getType());
			umamFile.put(partitionProcessedHeader[16], umam.getLabel());
			umamFile.put(partitionProcessedHeader[17], umam.getRank());
			umamFile.put(partitionProcessedHeader[18], input.getCreator());
			umamFile.put(partitionProcessedHeader[19], input.getContinent());
			umamFile.put(partitionProcessedHeader[20], input.getCountry());
			umamFile.put(partitionProcessedHeader[21], input.getRegion());
			umamFile.put(partitionProcessedHeader[22], input.getSettlement());
			umamFile.put(partitionProcessedHeader[23], items.size());
			umamFile.put(partitionProcessedHeader[24], input.getPid());
			umamFile.put(partitionProcessedHeader[25], umam.getPid());
			umamFile.put(partitionProcessedHeader[26], getFedoraURL(input.getPid()));
			umamFile.put(partitionProcessedHeader[27], getFedoraURL(umam.getPid()));

								
			// write UMAM files grouped by file path info to the partition processed file
			// refer to the property ""processed.partitions.path"
			
			try {
				this.partitionProcessedWriter.write(umamFile, partitionProcessedHeader,
						getPartitionProcessedFilesProcessors());
				this.partitionProcessedWriter.flush();
				
				// log global count
				LoaderStats.uploadedProcessedUMAMCount.increment(); 
				
				// log local count
				this.stats.uploadedProcessedUMAMFileCount.increment();
											
			} catch (IOException e) {
				log.error("Error occurred while writing processed data. "
						+ e.getMessage());
			}

		}

		log.info(this.stats.getCurrentStatistics());
		log.info(LoaderStats.getCurrentStatistics());
		
		//KAPService.writeStatsEntry(this.stats.getCurrentStatistics() + "\n" + LoaderStats.getCurrentStatistics());
		
	}

	// staged metadata
	public static String[] getPartitionStagedFilesHeader() {

		final String[] header = new String[] { "umdm_id", "umam_page_id",
				"collection", "series", "subseries", "box", "folder", "item",
				"title", "date", "size", "handle",  "Accession Number", "filename", "path",
				"umam_type", "label", "rank", "creator", "continent",
				"country", "region", "settlement", "umam_count" };

		return header;
	}

	public static String getFedoraURL(String pid) {
		return KAPService.getConnection() + "/get/" + pid;
	}
	
	public static CellProcessor[] getPartitionStagedFilesProcessors() {

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

	// processed metadata

	public static String[] getPartitionProcessedFilesHeader() {

		final String[] header = new String[] { "umdm_id", "umam_page_id",
				"collection", "series", "subseries", "box", "folder", "item",
				"title", "date", "size", "handle", "Accession Number", "filename", "path",
				"umam_type", "label", "rank", "creator", "continent",
				"country", "region", "settlement", "umam_count", "umdm_pid",
				"umam_pid", "fedora_umdm_url", "fedora_umam_url" };

		return header;
	}

	public static CellProcessor[] getPartitionProcessedFilesProcessors() {

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
				new Optional(), // accession number 12
				new NotNull(), // fileName 13
				new NotNull(), // filePath 14
				new NotNull(), // UMAM Type 1 5
				new NotNull(), // label 16
				new NotNull(), // rank 17
				new Optional(), // creator 18
				new Optional(), // continent 19
				new Optional(), // country 20
				new Optional(), // region 21
				new Optional(), // settlement 23
				new Optional(), // umam_count 24
				new Optional(), // umdm_pid 25
				new Optional(), // umam_pid 26
				new Optional(), // fedora_umdm_url 27
				new Optional(), // fedora_umam_url 28
		};

		return processors;

	}

	// errors meta data

	private static String[] getErrorLogHeader() {

		final String[] header = new String[] { "error_id", "message",
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
}
