package edu.umd.lims.fedora.kap;


import java.io.ByteArrayInputStream;

import java.io.File;

import java.io.InputStream;

import java.util.concurrent.Callable;

import java.util.concurrent.Future;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.StopWatch;
import org.dom4j.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.lims.fedora.api.DigitalObject;
import edu.umd.lims.fedora.api.FedoraAPIException;
import edu.umd.lims.fedora.api.FedoraObjectMap;
import edu.umd.lims.fedora.api.UMAMObject;
import edu.umd.lims.fedora.api.UMDCorrespondenceDigitalObject;
import edu.umd.lims.fedora.api.UMDImageUMAMObject;
import edu.umd.lims.fedora.api.UMDMContentType;
import edu.umd.lims.fedora.api.contenttype.FileMimeType;
import edu.umd.lims.fedora.api.contenttype.MimeTypeDetector;
import edu.umd.lims.fedora.api.contenttype.UMAMContentType;
import edu.umd.lims.fedora.api.umam.AdminMetadataInfo;

public class UMAMUploader implements Callable<UMAMContentObject> {

	private static final Logger log = LoggerFactory
			.getLogger(UMAMUploader.class);

	private final UMAMCSVInput umam;
	private final DigitalObject digitalObject;
	private final File file;
	private final String strTitle;
	private final static StopWatch timer = new StopWatch();
	private final UMAMContentObject umamContentObject = new UMAMContentObject();

	public UMAMUploader(UMAMCSVInput umam, DigitalObject digitalObject,
			File file, String strTitle) {
		this.umam = umam;
		this.digitalObject = digitalObject;
		this.file = file;
		this.strTitle = strTitle;

	}

	public UMAMContentObject call() throws Exception {

		timer.reset();
		timer.start();

		Exception exception = null;
		String errorMessage = null;

		String fileMimeType = MimeTypeDetector.getMimeType(file);
		log.info("UMAM File Mime Type: " + fileMimeType);

		String umamContentType = null;
		Document umamDocument = null;

		String contentLabel = umam.getUMAMContentLabelbyType();
		FedoraObjectMap page = new FedoraObjectMap("parts", null, "hasPart",
				umam.getRank(), umam.getLabel(), contentLabel);

		String fileName = file.getName();
		log.info("File Name: " + fileName);

		final InputStream inputStream = new ByteArrayInputStream(
				IOUtils.toByteArray(file.toURI()));

		umamContentType = UMDCorrespondenceDigitalObject
				.getUMAMContentType(contentLabel);

		if (umamContentType.equals(UMAMContentType.UMD_HOCR.name())) {
			fileMimeType = FileMimeType.text_html.getValue();
		}

		final String mimeType = fileMimeType;

		AdminMetadataInfo umamMetadata = KAPUploader.getAdminMetadata();
		String pid = null;
		try {

			final UMAMObject umamObject = digitalObject.addUMAM(
					umamContentType, null, page);
			pid = umamObject.getPid();

			if (umamMetadata != null) {
				log.debug("Generate Extended UMAM for Correspondence Object");
				umamDocument = umamObject.generateUMAM(file, fileName,
						mimeType, UMDMContentType.UMD_CORRESPONDENCE.name(),
						umamMetadata);
			} else {
				log.debug("Generate UMAM Object");
				umamDocument = umamObject
						.generateUMAM(file, fileName, mimeType);
			}

			umamObject.setUMAM(umamDocument);

			log.info("UMAM PID : = " + umamObject.getPid()
					+ " UMAM Mime Type: = " + mimeType);

			Future uploadTask = KAPService.getUploadPooll().submit((new Runnable() {

				@Override
				public void run() {
					try {
						umamObject.uploadContentItem(inputStream, mimeType);
					} catch (FedoraAPIException e) {

						String errorMessage = "Error uploading content. "
								+ umamObject.getPid()
								+ " Metadata input partition UMAM File: "
								+ umam.getPartition().getPartitionPath() + "; "
								+ "Line #: " + umam.getLineNumber()
								+ " Message: " + e.getMessage() + "; Cause: "
								+ e.getCause();
						e.printStackTrace();
						log.error(errorMessage);

						// write to global log
						LoaderStats.errorCount.increment();
						ErrorLogEntry error = new ErrorLogEntry(
								LoaderStats.errorCount.getValue(), errorMessage);
						KAPService.writeErrorLog(error);

						// write to local log
						umam.getPartition().writeErrorLog(error);
						umam.getPartition().getStats().errorCount.increment();

					}
				}

			}));

			while (true) {
				if (uploadTask.isDone()) {
					log.info("Content has been uploaded." + "UMAM PID : = "
							+ umamObject.getPid() + " UMAM Content : = "
							+ file.getAbsolutePath());
					IOUtils.closeQuietly(inputStream);

					if (umamObject instanceof UMDImageUMAMObject) {
						umamContentObject.setIsImage(true);
					}
			

					if (umamObject instanceof UMDImageUMAMObject) {
						UMDImageUMAMObject umdImage = (UMDImageUMAMObject) umamObject;
						Future<String> zoomifyTask = KAPService.getUploadPooll()
								.submit(new ZoomifyContent(umdImage, umam
										.getLineNumber(), umam.getPartition(),
										strTitle));

						while (true) {
							if (zoomifyTask.isDone()) {
								log.info("Content has been zoomified."
										+ "UMAM PID : = " + umamObject.getPid()
										+ " UMAM Content : = "
										+ file.getAbsolutePath());
								
								
								break;
								
								
							}
							if (!zoomifyTask.isDone()) {
								zoomifyTask.get();
								log.info("Waiting for content being zoomified. "
										+ "UMAM PID : = "
										+ umamObject.getPid()
										+ " UMAM Content : = "
										+ file.getAbsolutePath());
							}
						}
					}

					break;
				}

				if (!uploadTask.isDone()) {
					uploadTask.get();
					log.info("Waiting for content being uploaded. "
							+ "UMAM PID : = " + umamObject.getPid()
							+ " UMAM Content : = " + file.getAbsolutePath());
				}
			}

			timer.stop();
			log.info("UMAM uploading completed." + "UMAM PID : = "
					+ umamObject.getPid() + "; UMAM Content : = "
					+ file.getAbsolutePath());

		} catch (FedoraAPIException e) {

			exception = e;

			errorMessage = "Cannot create UMAM for Digital Object: "
					+ digitalObject.getPid()
					+ " Metadata input partition UMAM File: "
					+ umam.getPartition().getPartitionPath() + "; "
					+ "Line #: " + umam.getLineNumber()
					+ " Error occurred: Fedora API exception. " + " Message: "
					+ e.getMessage() + "; Cause: " + e.getCause();

			e.printStackTrace();

		} catch (Exception e) {
			exception = e;

			errorMessage = "Cannot create UMAM for Digital Object: "
					+ digitalObject.getPid()
					+ " Metadata input partition UMAM File: "
					+ umam.getPartition().getPartitionPath() + "; "
					+ "Line #: " + umam.getLineNumber()
					+ " Exception occurred. " + " Message: " + e.getMessage()
					+ "; Cause: " + e.getCause();

			e.printStackTrace();

		} finally {

			if (exception != null) {

				log.error(errorMessage);

				LoaderStats.errorCount.increment();
				ErrorLogEntry error = new ErrorLogEntry(
						LoaderStats.errorCount.getValue(), errorMessage);
				KAPService.writeErrorLog(error);

				// write to local log
				umam.getPartition().writeErrorLog(error);
				umam.getPartition().getStats().errorCount.increment();

				}

			if (inputStream != null) {
				IOUtils.closeQuietly(inputStream);
			}

		}

		umamContentObject.setPid(pid);
		umamContentObject.setId(umam.getId());

		return umamContentObject;

	}
	

}
