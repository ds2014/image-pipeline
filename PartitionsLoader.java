package edu.umd.lims.fedora.kap;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

public class PartitionsLoader {

	private static final Logger log = LoggerFactory
			.getLogger(PartitionsLoader.class);

	private final static StopWatch timer = new StopWatch();
	private static ICsvMapReader partitionsReader;
	private static ICsvMapWriter partitionsWriter;
	
	private static int maxRecordCount = 0;

	public static void loadPartitions() throws Exception {

		timer.reset();
		timer.start();

		maxRecordCount = LoaderStats.sourceUMDMCount.getValue();
		Exception exception = null;
		
		StatusEntry status = new StatusEntry(KAPService.getBatchPath(), "Incomplete");
		KAPService.writeBatchStatus(status);
		
		if (KAPService.getMaxRecordCount() > 0) {
			maxRecordCount = Math.min(LoaderStats.sourceUMDMCount.getValue(),
				KAPService.getMaxRecordCount());
		}
		
		FileReader fileReader;
		try {
			fileReader = new FileReader(KAPService.getStagedPartitionsPath()
					+ "/partitions-summary.csv");
			log.info("Reading partitions control file: "
					+ KAPService.getStagedPartitionsPath()
					+ "/partitions-summary.csv");
			
			log.info("Loading KAP Titles to Fedora has started....");
			
			partitionsReader = new CsvMapReader(fileReader,
					CsvPreference.STANDARD_PREFERENCE);
			partitionsReader.getHeader(true); // skip past the header
			
			FileWriter partitionsSummaryWriter = new FileWriter(
					KAPService.getProcessedPartitionsPath()+"/partitions-summary-processed.csv");
			
			partitionsWriter = new CsvMapWriter(partitionsSummaryWriter,
					CsvPreference.STANDARD_PREFERENCE);
			partitionsWriter.writeHeader(DatasetReader.getPartitionsSummaryHeader());
			
			
			final String[] header = DatasetReader.getPartitionsSummaryHeader();
			final CellProcessor[] processors = DatasetReader
					.getPartitionsSummaryProcessors();

			int umdmPartitionCount = 0;
			int umamPartitionCount = 0;

			Map<String, Object> partitionMap;
			

			while ((partitionMap = partitionsReader.read(header, processors)) != null && (LoaderStats.uploadedProcessedUMDMFileCount.getValue() < maxRecordCount)) {
				int partitionNumber = Integer.parseInt((String) partitionMap
						.get("part_id"));
				String partitionPath = (String) partitionMap.get("path");

				if (NumberUtils.isNumber((String) partitionMap
						.get("umdm_count"))) {
					umdmPartitionCount = Integer.parseInt((String) partitionMap
							.get("umdm_count"));
				}

				if (NumberUtils.isNumber((String) partitionMap
						.get("umam_count"))) {
					umamPartitionCount = Integer.parseInt((String) partitionMap
							.get("umam_count"));
				}

				Partition partition = new Partition(partitionNumber,
						partitionPath, umdmPartitionCount, umamPartitionCount);
				
				PartitionProcessor partitionProcessor = new PartitionProcessor(partition);
				partitionProcessor.setMaxRecordCount(maxRecordCount);
				partitionProcessor.loadPartition();
				
				writePartitionsSummary(partition);
				
			}

		} catch (FileNotFoundException e) {
			String errorMessage = "Error reading partition control file. FileNotFoundException. "
					+ KAPService.getStagedPartitionsPath()
					+ "/partitions-summary.csv";
			
			
			log.error(errorMessage);
			e.printStackTrace();

			// log global error
			LoaderStats.errorCount.increment();

			ErrorLogEntry error = new ErrorLogEntry(
					LoaderStats.errorCount.getValue(), errorMessage);
			KAPService.writeErrorLog(error);
			
			exception = new KAPServiceException("Error reading partition control file. FileNotFoundException. "
					+ KAPService.getStagedPartitionsPath()
					+ "/partitions-summary.csv", ServiceErrorCode.UMAM_MD_ERROR.getValue());
			
		
			
		} catch (IOException e) {
			String errorMessage = "Error reading partition control file. FileNotFoundException. "
					+ KAPService.getStagedPartitionsPath()
					+ "/partitions-summary.csv";
			
			
			log.error(errorMessage);
			e.printStackTrace();

			// log global error
			LoaderStats.errorCount.increment();

			ErrorLogEntry error = new ErrorLogEntry(
					LoaderStats.errorCount.getValue(), errorMessage);
			KAPService.writeErrorLog(error);
			
			exception = new KAPServiceException("Error reading partition control file. FileNotFoundException. "
					+ KAPService.getStagedPartitionsPath()
					+ "/partitions-summary.csv", ServiceErrorCode.UMAM_MD_ERROR.getValue());
		}
			finally{
				if (partitionsReader != null) {
					partitionsReader.close();
				}

				if (partitionsWriter != null) {
					partitionsWriter.close();
				}
				if (exception==null){
					status = new StatusEntry(KAPService.getBatchPath(), "Completed");
					KAPService.writeBatchStatus(status);
				}else{
					throw exception;
				}
			}
		
		timer.stop();

		log.info("Partitions Loading completed" + "; Total time taken. "
				+ timer.toString());

	}
	
  private static  void writePartitionsSummary(Partition partition){
		
		String [] header = DatasetReader.getPartitionsSummaryHeader();
		
		// add created partition to summary processed file
		final Map<String, Object> currentPartition = new HashMap<String, Object>();

		currentPartition.put(header[0], partition.getPartitionNumber());
		currentPartition.put(header[1], partition.getPartitionPath());
		currentPartition.put(header[2], partition.getStats().stagedUMDMTitleCount.getValue());
		currentPartition.put(header[3], partition.getStats().stagedUMAMFileCount.getValue());
		currentPartition.put(header[4], partition.getStats().uploadedProcessedUMDMTitleCount.getValue());
		currentPartition.put(header[5], partition.getStats().uploadedProcessedUMAMFileCount.getValue());
		currentPartition.put(header[6], partition.getStats().errorCount.getValue());
		
		try {
			partitionsWriter.write(currentPartition, header, DatasetReader.getPartitionsSummaryProcessors());
			partitionsWriter.flush();
		} catch (IOException e) {
			
			String errorMessage = "Cannot write processed partitions control information.";
						
			log.error(errorMessage);
			e.printStackTrace();

			// log global error
			LoaderStats.errorCount.increment();

			ErrorLogEntry error = new ErrorLogEntry(
					LoaderStats.errorCount.getValue(), errorMessage);
			KAPService.writeErrorLog(error);
			
		}
		
	}

}
