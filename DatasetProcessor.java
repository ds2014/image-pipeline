package edu.umd.lims.fedora.kap;

import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetProcessor {
	private static final Logger log = LoggerFactory.getLogger(DatasetProcessor.class);
	private final static StopWatch datasetProcessorTimer = new StopWatch();
	
	public static void processDataset() throws Exception{
		datasetProcessorTimer.reset();
		datasetProcessorTimer.start();
		log.info("Dataset Processing has started...");

	    DatasetReader.readDataset();
		
	    PartitionsLoader.loadPartitions();
		
		datasetProcessorTimer.stop();

		log.info("Dataset Processing Completed." + "Total time taken: " + datasetProcessorTimer.toString());
			
	}

}
