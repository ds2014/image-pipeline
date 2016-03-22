package edu.umd.lims.fedora.kap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

public class UMAMContentObject extends ContentObject{
	 
	private boolean isImage;
	HashMap <String,Future <String>> zoomifyTasks = new HashMap <String,Future <String>>();
	
	 public UMAMContentObject(){
		 
	 }
	 
	public UMAMContentObject(String title, String type, String pid) {
		
		super(type, type, pid );
			
	}

    public UMAMContentObject(String id, String title, String type, String pid) {
		
		super(type, type, pid );
		super.setId(id);
	
			
	}
    
    public boolean isImage(){
    	return isImage;
    }

    public void setIsImage(boolean isImage){
    	 this.isImage = isImage;
    }
    
    public String toString() {
    	return this.getId();
    }
    
    public HashMap <String,Future <String>> getZoomifyTasks(){
    	return this.zoomifyTasks;
    }
   
   
  }
