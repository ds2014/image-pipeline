package edu.umd.lims.fedora.kap;

public class ContentObject {

	private String title;
	private String pid;
	private String id;
	private String lineNumber;
	private String type;
	private String status;
	
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	
	public ContentObject() {

	}
	
	public ContentObject(String title, String type, String pid) {
		this.type = type;
		this.title = title;
		this.pid = pid;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	public String getId(){
		return this.id;
	}

	public void setId(String id){
		 this.id = id;
	}
}
