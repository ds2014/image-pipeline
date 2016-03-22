package edu.umd.lims.fedora.kap;

public enum ServiceErrorCode {

	FEDORA_CLIENT_EMPTY(0, "The Fedora client has not been initialized"), 
	FEDORA_CLIENT_ERROR(
			1, "Fedora Client cannot be initialized"),
	FEDORA_CLIENT_CONNECTION_ERROR(
					11, " Cannot connect to Fedora client."),
			FEDORA_API_ERROR(2,
			"Fedora API cannot be initialized"), FEDORA_UPLOADER_ERROR(3,
			"Fedora Uploader cannot be initialized"), HOST_ERROR(4,
			"Host cannot be initilaized"), DATA_PATH_ERROR(5,
			"Data directory does not exist or not readable."),

	MD_PATH_ERROR(6, "Metadata file does not exist or not readable."),

	LOG_PATH_ERROR(7, "Logs directory does not exist or not readable."),

	UMAM_MD_ERROR(8,
			"UMAM Processed Metadata file does not exist or not readable."),

	DSRD_ERROR(9, "Error while reading metadata input file"),

	ZOOM_ERROR(10, "Error attepmting zoomify image."),
	BATCH_DIR_ERROR(12, "Error creating batch directory."),
	BATCH_DIR_NOT_EXIST(13, "Batch directory does not exists.");
	;

	private int code;
	private String name;

	private ServiceErrorCode(int code, String name) {
		this.code = code;
		this.name = name;
	}

	public String getValue() {
		return this.name;
	}
}
