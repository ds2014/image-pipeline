package edu.umd.lims.fedora.kap;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.lims.fedora.api.FedoraAPIException;
import edu.umd.lims.fedora.api.UMDImageUMAMObject;

public class ZoomifyContent implements Callable<String> {

	private static final Logger log = LoggerFactory
			.getLogger(ZoomifyContent.class);

	private UMDImageUMAMObject umdImage;
	private int lineNumber;
	private Partition partition;
	private String strTitle;

	ZoomifyContent(UMDImageUMAMObject umdImage) {
		this.umdImage = umdImage;
	}
	
	ZoomifyContent(UMDImageUMAMObject umdImage, int lineNumber, Partition partition, String strTitle) {
		this.umdImage = umdImage;
		this.lineNumber = lineNumber;
		this.partition = partition;
		this.strTitle = strTitle;
	}
	
	public String call() {
		Exception ex = null;
		String result = null;
		HttpURLConnection conn = null;
		try {
			URL url = getZoomifyURL();

			InputStream inputStream = null;
			conn = (HttpURLConnection) url.openConnection();

			if (conn.getResponseCode() != 200) {
				ex = new IOException(conn.getResponseMessage());
			}

			inputStream = conn.getInputStream();

			SAXReader xmlReader = new SAXReader();
			Document zoomDoc = null;
			zoomDoc = xmlReader.read(inputStream);

			if (!umdImage.hasZoomify()) {

				log.info("Adding zoomify data stream for: " + umdImage.getPid());
				umdImage.addDatastream("zoom", "Zoomify", false, "text/xml",
						"X", zoomDoc, null);

				// add zoomify disseminator

				log.info("Adding zoomify disseminator for: "
						+ umdImage.getPid());
				HashMap bindings = new HashMap();
				bindings.put("zoom", "zoom");
				umdImage.addDisseminator("umd-bdef:zoomify",
						"umd-bmech:zoomify.xml", bindings);
			}

			IOUtils.closeQuietly(inputStream);
			
		} catch (MalformedURLException e) {
			ex = e;
		} catch (IOException e) {
			ex = e;
		} catch (DocumentException e) {
			ex = e;
		} catch (FedoraAPIException e) {
			ex = e;
		} 	
		finally {
			
			if (conn!=null){
				conn.disconnect();
			}

			if (ex == null) {
				result = umdImage.getPid();
			} else {
				String errorMessage = "Cannot add zoomify for: " + umdImage.getPid() +
						" Metadata input partition UMAM File: " + this.partition.getPartitionPath() + "; " +
						"Line #: " + this.lineNumber
					 + " Message: " + ex.getMessage()
					 + "; Cause: " + ex.getCause();
				ex.printStackTrace();
				log.error(errorMessage);
				
				// write to global log
				LoaderStats.errorCount.increment();
				ErrorLogEntry error = new ErrorLogEntry(
						LoaderStats.errorCount.getValue(), errorMessage);
				KAPService.writeErrorLog(error);
				
				
				// write to local log
				this.partition.writeErrorLog(error);
				this.partition.getStats().errorCount.increment();

			}
		}

		return result;

	}

	private URL getZoomifyURL() throws MalformedURLException, UnsupportedEncodingException {
		
		String url = "http://" + KAPService.getHost() + "/services/ZoomifyImage?pid="
				+ this.umdImage.getPid();
		
		if( strTitle != null && ( strTitle.length() > 0 ) ) {
			try {
				url += "&title=" + URLEncoder.encode(strTitle, "UTF-8");
			} catch (UnsupportedEncodingException e) {
		
				log.debug("Title had a bad URL encoding.");
				e.printStackTrace();
				
				throw e;
			}
		}
				
		log.info("Zoomify URL: " + url);
		
		return new URL(
				url);
	}
	
	public String getPid(){
		return this.umdImage.getPid();
	}

}
	
