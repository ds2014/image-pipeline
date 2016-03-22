package edu.umd.lims.fedora.kap;

public class ErrorLogEntry {
	private int errorId;
	private String message;
	
	public ErrorLogEntry (String message) {
		this.message = message;
	}
	
	public ErrorLogEntry(int errorId, String message) {
		this.errorId = errorId;
		this.message = message;
	}
	
	public int getErrorId() {
		return errorId;
	}
	public void setErrorId(int errorId) {
		this.errorId = errorId;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}

}
