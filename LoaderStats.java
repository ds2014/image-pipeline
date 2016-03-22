package edu.umd.lims.fedora.kap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

public class LoaderStats {

	private static final Logger log = LoggerFactory
			.getLogger(LoaderStats.class);

	protected static Counter sourceUMDMCount = new Counter();
	protected static Counter stagedUMDMCount = new Counter();
	protected static Counter sourceCount = new Counter();

	protected static Counter sourceUMAMPagesCount = new Counter();
	protected static Counter stagedUMAMPagesCount = new Counter();

	protected static Counter masterProcessedUMDMCount = new Counter();
	protected static Counter masterProcessedUMAMFileCount = new Counter();
	protected static Counter masterProcessedCount = new Counter();

	protected static Counter uploadedProcessedUMDMFileCount = new Counter();
	protected static Counter uploadedProcessedUMAMCount = new Counter();

	protected static Counter errorCount = new Counter();
	protected static Counter umdmErrorCount = new Counter();
	protected static Counter umamErrorCount = new Counter();

	protected static Counter partitionCount = new Counter();
	private static ICsvMapReader partitionReader;

	public Counter getTotalRecordProcessed() {
		return uploadedProcessedUMAMCount;
	}

	public static String getStatistics() {

		StringBuilder result = new StringBuilder("Loader Statistics: " + "\n");

		result.append("Total Source Record Count: " + sourceCount.getValue()
				+ "\n");
		result.append("Total Source UMDM Record Count: "
				+ sourceUMDMCount.getValue() + "\n");
		result.append("Total Source UMAM Pages Count: "
				+ stagedUMAMPagesCount.getValue() + "\n");

		result.append("Total Preprocessed UMAM Files Count: "
				+ masterProcessedUMAMFileCount.getValue() + "\n");

		result.append("Total Uploaded UMDM Count: "
				+ uploadedProcessedUMDMFileCount.getValue() + "\n");
		result.append("Total Uploaded UMAM Count: "
				+ uploadedProcessedUMAMCount.getValue() + "\n");

		result.append("Total Error Count: " + errorCount.getValue());

		if (result != null) {
			return result.toString();
		} else {
			return null;
		}

	}

	public static String getCurrentStatistics() {
		StringBuilder result = new StringBuilder("Current Loader Statistics: "
				+ "\n");

		result.append("Total KAP Objects uploaded "
				+ LoaderStats.uploadedProcessedUMDMFileCount.getValue()
				+ " of Total KAPs "
				+ LoaderStats.sourceUMDMCount.getValue()
				+ " Completed: "
				+ getPercent(
						LoaderStats.uploadedProcessedUMDMFileCount.getValue(),
						LoaderStats.sourceUMDMCount.getValue()) + "\n");

		result.append("Total UMAM Objects uploaded "
				+ LoaderStats.uploadedProcessedUMAMCount.getValue()
				+ " of Total UMAMs "
				+ LoaderStats.masterProcessedUMAMFileCount.getValue()
				+ " Completed: "
				+ getPercent(LoaderStats.uploadedProcessedUMAMCount.getValue(),
						LoaderStats.masterProcessedUMAMFileCount.getValue()));

		if (result != null) {
			return result.toString();
		} else {
			return null;
		}
	}

	public static void getPartitionsSummaryStatistics() {
		StringBuilder result = new StringBuilder(
				"Summary Statistics by Partition : " + "\n");
		result.append("---------------------------------");

		if (result != null) {
			log.info(result.toString());
		}

		Exception exception = null;

		FileReader fileReader;

		try {
			fileReader = new FileReader(KAPService.getProcessedPartitionsPath()
					+ "/partitions-summary-processed.csv");
       
			partitionReader = new CsvMapReader(fileReader,
					CsvPreference.STANDARD_PREFERENCE);
			partitionReader.getHeader(true);

			final String[] header = DatasetReader.getPartitionsSummaryHeader();
			final CellProcessor[] processors = DatasetReader
					.getPartitionsSummaryProcessors();

			Map<String, Object> partitionMap;

			while ((partitionMap = partitionReader.read(header, processors)) != null) {
				printStaticticsbyPartition(partitionMap);
			}
		} catch (NumberFormatException e) {
			exception = e;
		} catch (IOException e) {
			exception = e;
		} finally {
			if (exception != null) {

				String errorMessage = "Error printing summary statistics by partition. "

						+ ";  "
						+ "Message: "
						+ exception.getMessage()
						+ "; Cause: " + exception.getCause();

				log.error(errorMessage);
				exception.printStackTrace();

				// log global error
				LoaderStats.errorCount.increment();
				ErrorLogEntry error = new ErrorLogEntry(
						LoaderStats.errorCount.getValue(), errorMessage);
				KAPService.writeErrorLog(error);

			}
		}

	}

	public static void printStaticticsbyPartition(final Map partitionMap) {

		int umdmPartitionCount = 0;
		int umamPartitionCount = 0;
		
		int actual_umdmPartitionCount = 0;
		int actual_umamPartitionCount = 0;
		
		int errorCount = 0;

		int partitionNumber = 0;
		String partitionPath = "";

		try {

			partitionNumber = Integer.parseInt((String) partitionMap
					.get("part_id"));
			partitionPath = (String) partitionMap.get("path");

			if (NumberUtils.isNumber((String) partitionMap.get("umdm_count"))) {
				umdmPartitionCount = Integer.parseInt((String) partitionMap
						.get("umdm_count"));
			}

			if (NumberUtils.isNumber((String) partitionMap.get("actual_umdm_count"))) {
				actual_umdmPartitionCount = Integer.parseInt((String) partitionMap
						.get("actual_umdm_count"));
			}
			
			if (NumberUtils.isNumber((String) partitionMap.get("umam_count"))) {
				umamPartitionCount = Integer.parseInt((String) partitionMap
						.get("umam_count"));
			}

			if (NumberUtils.isNumber((String) partitionMap.get("actual_umam_count"))) {
				actual_umamPartitionCount = Integer.parseInt((String) partitionMap
						.get("actual_umam_count"));
			}
			
			if (NumberUtils.isNumber((String) partitionMap.get("error_count"))) {
				errorCount = Integer.parseInt((String) partitionMap
						.get("error_count"));
			}
			
			Partition partition = new Partition(partitionNumber, partitionPath
					);

			partition.getStats().stagedUMDMTitleCount.setValue(umdmPartitionCount);
			partition.getStats().stagedUMAMFileCount.setValue(umamPartitionCount);
			partition.getStats().errorCount.setValue(errorCount);
			partition.getStats().uploadedProcessedUMDMTitleCount.setValue(actual_umdmPartitionCount);
			partition.getStats().uploadedProcessedUMAMFileCount.setValue(actual_umamPartitionCount);
			
			
			
			log.info(partition.getStats().getSummaryStatictics(partition));
			
		} catch (Exception e) {
			String errorMessage = "Error printing summary statistics for partition. "
					+ "Partition #: "
					+ partitionNumber
					+ "; "
					+ "Partition path: "
					+ partitionPath
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
		}
	}

	public static String getPercent(int count, int totalCount) {
		double percent = 0;
		String result = "";

		if (totalCount != 0) {
			percent = (new Double(count) / totalCount);
		}

		NumberFormat percentFormatter = NumberFormat.getPercentInstance();
		result = percentFormatter.format(percent);
		return result;

	}
}
