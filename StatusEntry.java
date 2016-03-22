package edu.umd.lims.fedora.kap;

public class StatusEntry {
	
	private String batchPath;
	private String end;
	private String status;
	
	
	public StatusEntry(String batchPath, String status){
		this.batchPath = batchPath;
		this.status = status;
	}
	
	public String getBatchPath() {
		return batchPath;
	}
	public void setBatchPath(String batchPath) {
		this.batchPath = batchPath;
	}
	
	public String getEnd() {
		return end;
	}
	public void setEnd(String end) {
		this.end = end;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}

}
