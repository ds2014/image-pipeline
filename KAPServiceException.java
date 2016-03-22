package edu.umd.lims.fedora.kap;

public class KAPServiceException extends Exception{

	private String errorCode = "Unknown_Exception";
	
	public KAPServiceException() {
		
	}
	
    public KAPServiceException(String message)
    {
       super(message);
    }
    
    public KAPServiceException(String message, String errorCode)
    {
       super(message);
       this.errorCode = errorCode;
    }
    
    public String getErrorCode(){
    	return this.errorCode;
    }
}
