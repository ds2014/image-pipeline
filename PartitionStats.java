package edu.umd.lims.fedora.kap;

import java.text.NumberFormat;

public class PartitionStats {
	
	protected Counter stagedUMDMTitleCount = new Counter();
	protected Counter stagedUMAMFileCount = new Counter();
	
	protected Counter uploadedProcessedUMDMTitleCount = new Counter();
	protected Counter uploadedProcessedUMAMFileCount = new Counter();
	
	protected Counter errorCount = new Counter();
	protected Counter umdmTitleErrorCount = new Counter();
	protected Counter umamFileErrorCount = new Counter();

	
	public  String getCurrentStatistics(){
		StringBuilder result = new StringBuilder("Current Partition Statistics: " + "\n"
				);

	result.append("Total UMDM Titles uploaded " + uploadedProcessedUMDMTitleCount.getValue()+ " of Total KAPs " 
				+stagedUMDMTitleCount.getValue() + " Completed: " 
				+ getPercent(uploadedProcessedUMDMTitleCount.getValue(),stagedUMDMTitleCount.getValue())+ "\n");
		
		result.append("Total UMAMs uploaded " + LoaderStats.uploadedProcessedUMAMCount.getValue() + " of Total UMAMs " + stagedUMAMFileCount.getValue() +
				" Completed: " + getPercent(uploadedProcessedUMAMFileCount.getValue(),stagedUMAMFileCount.getValue()));	
		
				
		if (result != null) {
			return result.toString();
		} else {
			return null;
		}
	}
	
  public String getSummaryStatictics(final Partition partition){
	  StringBuilder result = new StringBuilder("Partition Statistics: "
				+ "\n");

	  	result.append("Partition #: " + partition.getPartitionNumber() + "; " + " Partition path: " + partition.getPartitionPath());
	  	
	  	result.append("; Total Partition UMDM Title Count: " + partition.getStats().stagedUMDMTitleCount.getValue()
				+ "\n");
	  	
		result.append("Total UMAM Files Count: "
				+ partition.getStats().stagedUMAMFileCount.getValue()
				+ "\n");
		

		result.append("Total Uploaded UMDM Count: "
				+ partition.getStats().uploadedProcessedUMDMTitleCount.getValue() + "\n");
		
		result.append("Total Uploaded UMAM Count: "
				+ partition.getStats().uploadedProcessedUMAMFileCount.getValue() + "\n");

		result.append("Total Error Count: " + partition.getStats().errorCount.getValue());
		
		result.append("---------------------------------");

		if (result != null) {
			return result.toString();
		} else {
			return null;
		}
	  
  }
	
	public static String getPercent(int count, int totalCount){
		double percent = 0;
		 String result = "";
		
		if (totalCount!=0){
			percent = ( new Double(count)/totalCount);
		}

	    NumberFormat percentFormatter = NumberFormat.getPercentInstance();
	    result = percentFormatter.format(percent);
		return result;
		
	}
	
}
