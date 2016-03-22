package edu.umd.lims.fedora.kap;

import edu.umd.lims.fedora.api.contenttype.UMAMContentLabels;

public class UMAMCSVInput extends UMDMCSVInput implements Comparable<UMDMCSVInput> {
	
	private String parentId;
	private String filePath;
	private Partition partition;
		
	public UMAMCSVInput(String parentId) {
		super();
		this.parentId = parentId;
	}
	
	public UMAMCSVInput (String parentId, String id, String fileName) {
		super(id, fileName);
		this.parentId = parentId;
	}
	
	
	public UMAMCSVInput (String parentId, String id, String fileName, String label, String rank) {
		super(id, fileName, label, rank);
		this.parentId = parentId;
	}
	
	public UMAMCSVInput (String parentId, String id, String fileName, String label, String rank, String filePath) {
		super(id, fileName, label, rank);
		this.parentId = parentId;
		this.filePath = filePath;
	}
	
	public UMAMCSVInput (String parentId, String id, String fileName, String label, String rank, String filePath, String type) {
		super(id, fileName, label, rank, type);
		this.parentId = parentId;
		this.filePath = filePath;
	}
	
	public String getParentId() {
		return parentId;
	}
	
	public void setParentId(String parentId){
		this.parentId = parentId;
	}
	
	public String getFilePath(){
		return this.filePath;
	}
		
	public void setFilePath(String filePath){
		 this.filePath = filePath;
	}
	
	public Partition getPartition(){
		return this.partition;
	}
	
	public void setPartition(Partition partition){
		this.partition = partition;
	}
	
	public String toString() {

		StringBuilder objectName = new StringBuilder("Parent ID = "
				+ this.getParentId() + " Object ID = " +
						this.getId() + " Label =" + this.getLabel() + " Rank = " + this.getRank());
		
		if (this.getFilePath() != null) {
			objectName = objectName.append("; File Path = ")
					.append(this.getFilePath());
		}
		
		if (objectName != null) {
			return objectName.toString();
		} else {
			return null;
		}
		

	}


	public String getUMAMContentLabelbyType(){
	
		String result = null;
		
		if (getType().equals("jpg")){
			result =  UMAMContentLabels.DISPLAY.toString();
		}
		else if (getType().equals("ocr")) {
			
			result =  UMAMContentLabels.OCR.toString();
		}else if (getType().equals("hocr")) {
			
			result =  UMAMContentLabels.hOCR.toString();
		}else{
			return "Unknown content type";
		}
		
		return result;
	}
	
	@Override
	public int compareTo(UMDMCSVInput other) {
		
		Integer rank1 = Integer.parseInt(this.getRank());
		Integer rank2 = Integer.parseInt(other.getRank());
		
		 int cmp = rank1.compareTo(rank2);
		 
		    if (cmp != 0) {
		      return cmp;
		    }
		    return 0;
	}
	
	
}
