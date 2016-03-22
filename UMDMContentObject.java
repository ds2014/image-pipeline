package edu.umd.lims.fedora.kap;

import java.util.ArrayList;
import java.util.List;

public class UMDMContentObject extends ContentObject{
	 
	 private List<UMAMContentObject> contentItems = new ArrayList<UMAMContentObject>();
	 private UMDMCSVInput sourceUMDM; 
	 
	 public List<UMAMContentObject> getContentItems() {
		return contentItems;
	}

	public void setContentItems(List<UMAMContentObject> contentItems) {
		this.contentItems = contentItems;
	}

	public UMDMContentObject(){
		 
	 }
	 
	public UMDMContentObject(String title, String type, String pid) {
		
		super(title, type, pid );
			
	}
	
	public int getTotalContentItemsCount(){
		return this.getContentItems().size();
	}
	
	public UMDMCSVInput getSourceUMDM(){
		return this.sourceUMDM;
	}
	
	public void setSourceUMDM(UMDMCSVInput sourceUMDM){
		 this.sourceUMDM = sourceUMDM;
	}

}
