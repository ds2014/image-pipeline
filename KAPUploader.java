package edu.umd.lims.fedora.kap;

import java.io.File;
import java.io.IOException;

import java.util.HashMap;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.time.StopWatch;
import org.dom4j.Document;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.lims.fedora.api.DigitalObject;
import edu.umd.lims.fedora.api.UMDCorrespondenceDigitalObject;
import edu.umd.lims.fedora.api.umam.AdminMetadataInfo;
import edu.umd.lims.fedora.api.umam.DigitalProvider;
import edu.umd.lims.fedora.api.umam.Image;
import fedora.client.FedoraClient;
import fedora.client.Uploader;
import fedora.server.access.FedoraAPIA;
import fedora.server.management.FedoraAPIM;

public class KAPUploader implements Callable<UMDMContentObject> {

	private static final Logger log = LoggerFactory
			.getLogger(KAPUploader.class);

	private final static StopWatch kapUploaderTimer = new StopWatch();
	private final static StopWatch connectionTimer = new StopWatch();

	private final UMDMCSVInput kapObject;
	private String[] collection;

	private FedoraClient client = null;
	private FedoraAPIA APIA = null;
	private FedoraAPIM APIM = null;
	private Uploader uploader = null;
	private int completedZoomifyTasks = 0;
	private int umamCount = 0;

	/**
	 * private static ExecutorService zoomifyPool = new LoaderThreadPool( 2, //
	 * core thread pool size 10, // maximum thread pool size 1, // time to wait
	 * before resizing pool TimeUnit.SECONDS, new
	 * ArrayBlockingQueue<Runnable>(1, true), new
	 * ThreadPoolFactory("Zoomify Pool"));
	 * 
	 * 
	 * private static CompletionService<String> zoomifyCompletionService = new
	 * ExecutorCompletionService<String>( zoomifyPool);
	 **/

	/**
	 * private ExecutorService umamPool = new LoaderThreadPool( 3, // core
	 * thread pool size 6, // maximum thread pool size 1, // time to wait before
	 * resizing pool TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(3,
	 * true), new ThreadPoolFactory("UMAM Pool"));
	 */

	private ExecutorService connectionPool = new LoaderThreadPool(
			1, // core thread pool size
			1, // maximum thread pool size
			1, // time to wait before resizing pool
			TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1, true),
			new ThreadPoolFactory("Connection Pool"));

	HashMap<String, Future<String>> titleZoomifyTasks = new HashMap<String, Future<String>>();

	KAPUploader(UMDMCSVInput kapObject) {
		this.kapObject = kapObject;
		this.umamCount = kapObject.getChildUMAM().size();
		this.titleZoomifyTasks.clear();
	}

	KAPUploader(UMDMCSVInput kapObject, String[] collection) {

		this.kapObject = kapObject;
		this.collection = collection;
		this.umamCount = kapObject.getChildUMAM().size();
		titleZoomifyTasks.clear();

	}

	public UMDMContentObject call() throws Exception {

		kapUploaderTimer.reset();
		kapUploaderTimer.start();

		validateClientConnection();
		
		Properties p = UtilsProperties.getProperties();
		String targetStatus = p.getProperty("targetStatus");
		
		// Status defaults to Pending without any other input
		if( targetStatus == null || targetStatus.length() < 1 ) {
			targetStatus = "Pending";
		}

		DigitalObject digitalObject = null;

		if (KAPService.getPort() > 0) {
			String port = Integer.toString(KAPService.getPort());
			log.debug("Create with port and host: " + "Host: = "
					+ KAPService.getHost() + "Port : = " + port);

			digitalObject = UMDCorrespondenceDigitalObject.create(
					KAPService.getHost(), port, KAPService.getAPIM(),
					KAPService.getAPIA(), KAPService.getUploader());
		} else {
			log.debug("Create with  host: " + "Host: = " + KAPService.getHost());
			digitalObject = UMDCorrespondenceDigitalObject.create(
					KAPService.getHost(), KAPService.getAPIM(),
					KAPService.getAPIA(), KAPService.getUploader());
		}

		String umdmPid = digitalObject.getPid();
		log.info("KAP Digital Object UMDM pid: " + umdmPid);

		log.debug("Generating UMDM Metadata document for KAP UMDM pid: "
				+ umdmPid);
		Document umdmDocument = digitalObject.getUMDM();
		umdmDocument = generateUMDMDocumentbyMetadata(umdmDocument);
		digitalObject.setUMDM(umdmDocument);
		digitalObject.setCollections(collection);
		TreeMap<String, UMAMCSVInput> items = kapObject.getChildUMAM();

		for (Map.Entry<String, UMAMCSVInput> item : items.entrySet()) {

			UMAMCSVInput umam = item.getValue();
			log.info("Adding content item : " + umam.getFilePath() + "; Page: "
					+ umam.getLabel() + "; Rank: " + umam.getRank());

			final File file = new File(umam.getFilePath());

			log.info("File path: " + file.getPath());

			validateClientConnection();

			final Future<UMAMContentObject> umamTask = KAPService
					.getUMAMPooll().submit(
							new UMAMUploader(umam, digitalObject, file,
									kapObject.getMainTitle()));

			UMAMContentObject umamContent = getUMAMContent(umamTask,
					umam.getFilePath());

			umam.setPid(umamContent.getPid());

			if (umamContent.getZoomifyTasks().size() > 0) {

				for (Map.Entry<String, Future<String>> task : umamContent
						.getZoomifyTasks().entrySet()) {
					titleZoomifyTasks
							.put(umamContent.getPid(), task.getValue());
				}

			}

			log.info("UMAM Content has been uploaded:" + umam.getFilePath());

		}

		if (digitalObject.hasEnoughContent()) {
			log.debug("UMDM: Pid: " + umdmPid + " has enough content");
			digitalObject.setStatus(targetStatus);
		} else {
			log.debug("UMDM: Pid: " + umdmPid + " has not enough content");
		}

		UMDMContentObject umdmContent = new UMDMContentObject();
		umdmContent.setPid(umdmPid);
		kapObject.setPid(umdmPid);
		umdmContent.setSourceUMDM(kapObject);
		umdmContent.setId(kapObject.getId());
		umdmContent.setStatus(targetStatus);

		kapUploaderTimer.stop();

		log.info("KAP Loading completed. Digital Object UMDM Pid: " + umdmPid
				+ "; Total time taken. " + kapUploaderTimer.toString());

		shutdownPools();

		/**
		 * while (titleZoomifyTasks.entrySet().size() == completedZoomifyTasks)
		 * { log.info("Waiting for completion of Zoomify tasks for UMDM: " +
		 * digitalObject.getPid() + zoomifyPool.toString() +
		 * "# Completed tasks: " + completedZoomifyTasks); }
		 **/

		// zoomifyPool.shutdown();

		return umdmContent;
	}

	private Document generateUMDMDocumentbyMetadata(Document document) {

		Document umdmDocument = document;
		Element root = umdmDocument.getRootElement();

		if (kapObject.getMainTitle() != null) {
			root.addElement("title").addAttribute("type", "main")
					.addText(kapObject.getMainTitle());
		} else {
			log.error("UMDM Title is not defined. Adding Unknown title.");
			root.addElement("title").addAttribute("type", "main")
					.addText("Unknown title");
		}

		if (kapObject.getCreator() != null) {
			Element creator = root.addElement("agent").addAttribute("type",
					"creator");

			Element person = creator.addElement("agent").addAttribute("type",
					"creator");
			person.addText(kapObject.getCreator());
		}

		if (kapObject.getDescription() != null) {
			root.addElement("description").addAttribute("type", "summary")
					.addText(kapObject.getDescription());
		}

		if (UMDMCSVInput.getRights() != null) {
			root.addElement("rights").addText(UMDMCSVInput.getRights());
		}

		if (UMDMCSVInput.getCopyRightOwner() != null) {
			root.addElement("rights").addAttribute("type", "copyrightowner")
					.addText(UMDMCSVInput.getCopyRightOwner());
		}

		root.addElement("mediaType").addAttribute("type", "text")
				.addElement("form").addAttribute("type", "analog")
				.addText("Letters");

		Element coverage = root.addElement("covPlace");

		if (kapObject.getContinent() != null) {
			coverage.addElement("geogName").addAttribute("type", "continent")
					.addText(kapObject.getContinent());
		}

		if (kapObject.getCountry() != null) {
			coverage.addElement("geogName").addAttribute("type", "country")
					.addText(kapObject.getCountry());
		}

		if (kapObject.getRegion() != null) {
			coverage.addElement("geogName").addAttribute("type", "region")
					.addText(kapObject.getRegion());
		}

		if (kapObject.getSettlement() != null) {
			coverage.addElement("geogName").addAttribute("type", "settlement")
					.addText(kapObject.getSettlement());
		}

		Element coverageTime = root.addElement("covTime");

		Element century = coverageTime.addElement("century");

		// Put the date figure(s) in if it exists
		century.addAttribute("certainty", "exact");
		century.addAttribute("era", "ad");
		century.addText("1901-2000");

		String baseDate = kapObject.getDate();
		String[] dateParts = baseDate.split(" ");
		String certainty = "circa";
		String fromDate = null;
		String toDate = null;
		String dateElement = "date";

		if (dateParts.length > 1) {
			if (dateParts[0].equalsIgnoreCase("undated")) {
				certainty = "unknown";
			} else if (dateParts[0].equalsIgnoreCase("circa")) {
				fromDate = dateParts[1];
				if (dateParts.length > 2) {
					toDate = dateParts[2];
				}
			} else if (dateParts[0].equalsIgnoreCase("before")) {
				fromDate = "1920";
				toDate = dateParts[1];
			} else if (dateParts[0].equalsIgnoreCase("after")) {
				fromDate = dateParts[1];
				toDate = "1980";
			} else {
				certainty = "exact";
				fromDate = dateParts[0];
				toDate = dateParts[1];
			}

			// Swap the dates if they were entered in the wrong order
			if (toDate != null && toDate.length() > 0) {
				dateElement = "dateRange";
				if (toDate.compareTo(fromDate) < 0) {
					String tempDate = toDate;
					toDate = fromDate;
					fromDate = tempDate;
				}
			}
		} else {
			if (dateParts[0].equalsIgnoreCase("undated")) {
				certainty = "unknown";
			} else {
				certainty = "exact";
				fromDate = dateParts[0];
			}
		}

		if (!certainty.equals("unknown")) {
			Element date = coverageTime.addElement(dateElement);

			date.addAttribute("certainty", certainty);
			date.addAttribute("era", "ad");

			if (dateElement.equals("dateRange")) {
				date.addAttribute("from", fromDate);
				date.addAttribute("to", toDate);
				date.addText(fromDate + " - " + toDate);
			} else {
				date.addText(fromDate);
			}
		}

		root.addElement("culture").addText("American");
		root.addElement("language").addText("en");

		Element repository = root.addElement("repository");

		repository.addElement("corpName").addText("Literary Manuscripts");

		Element physicalDesc = root.addElement("physDesc");

		if (kapObject.getSize() != null) {
			physicalDesc.addElement("extent").addAttribute("units", "pages")
					.addText(kapObject.getSize());
		}

		physicalDesc.addElement("color").addText("monochrome");

		root.addElement("subject").addAttribute("type", "browse")
				.addText("Literature, Print Culture");

		root.addElement("subject").addAttribute("type", "browse")
				.addText("Women's Studies");

		Element topicalSubject = root.addElement("subject").addAttribute(
				"scheme", "LCSH");
		topicalSubject.addAttribute("type", "topical").addText(
				"Porter, Katherine Anne, (1890-1980) -- Correspondence");

		Element relationships = root.addElement("relationships");
		Element relation = relationships.addElement("relation");
		relation.addAttribute("label", "archivalcollection");
		relation.addAttribute("type", "isPartOf");

		Element bibRef = relation.addElement("bibRef");

		if (kapObject.getCollection() != null) {
			bibRef.addElement("title").addAttribute("type", "main")
					.addText(kapObject.getCollection());
		}

		if (kapObject.getSeries() != null) {
			bibRef.addElement("bibScope").addAttribute("type", "series")
					.addText(kapObject.getSeries());
		}

		if (kapObject.getSubseries() != null) {
			bibRef.addElement("bibScope").addAttribute("type", "subseries")
					.addText(kapObject.getSubseries());
		}

		if (kapObject.getBox() != null) {
			bibRef.addElement("bibScope").addAttribute("type", "box")
					.addText(kapObject.getBox());
		}

		if (kapObject.getFolder() != null) {
			bibRef.addElement("bibScope").addAttribute("type", "folder")
					.addText(kapObject.getFolder());
		}

		if (kapObject.getItem() != null) {
			bibRef.addElement("bibScope").addAttribute("type", "item")
					.addText(kapObject.getItem());
		}

		if (kapObject.getAccession() != null) {
			bibRef.addElement("bibScope").addAttribute("type", "accession")
					.addText(kapObject.getAccession());
		}

		return umdmDocument;

	}

	public static AdminMetadataInfo getAdminMetadata() {

		AdminMetadataInfo umamMetadata = new AdminMetadataInfo();
		umamMetadata.setImage(getImage());
		umamMetadata.setProvider(getDigitalProvider());
		return umamMetadata;
	}

	public static DigitalProvider getDigitalProvider() {
		DigitalProvider provider = new DigitalProvider("2013-09-28",
				"The Crowley Company");
		return provider;
	}

	public static Image getImage() {
		Image image = new Image();
		image.setResolution("400");
		return image;
	}

	/* Fedora connection methods */

	private void initClient() throws IOException, KAPServiceException {

		log.info("Initializing Fedora client...");

		String connection = getConnection();
		log.info("Fedora Client Connection: " + connection);

		log.info("Connecting as user: " + KAPService.getUser());

		FedoraClient.FORCE_LOG4J_CONFIGURATION = false;

		client = new FedoraClient(connection, KAPService.getUser(),
				KAPService.getPassword());

		if (!canConnect()) {
			throw new KAPServiceException("Error connecting to Fedora client: "
					+ getConnection(),
					ServiceErrorCode.FEDORA_API_ERROR.getValue());
		}

		// TCP connection timeout 10 min (establishment of the TCP connection)
		client.getHttpClient().getHttpConnectionManager().getParams()
				.setConnectionTimeout(10 * 60 * 1000);
		// Socket timeout 10 min (generate connection timeout) if there is no
		// incomimg data flow within 10 min
		client.getHttpClient().getHttpConnectionManager().getParams()
				.setSoTimeout(10 * 60 * 1000);
		// Set max connection count
		client.getHttpClient().getHttpConnectionManager().getParams()
				.setMaxTotalConnections(10);

		log.info("Fedora client has been inialized. Fedora client endpoint url: "
				+ client.getUploadURL());

	}

	public int getFedoraClientStatus() {

		GetMethod getMethod = new GetMethod(getConnection());
		int responseCode = 0;

		try {
			if (client != null) {
				responseCode = client.getHttpClient().executeMethod(getMethod);
			} else {
				log.error("Fedora client has not been initialized.");
			}
		} catch (IOException e) {
			log.error("Cannot get Fedora client status");
		}

		log.info("Client response code: " + responseCode);

		return responseCode;
	}

	public boolean canConnect() {

		boolean result = false;
		if (getFedoraClientStatus() == HttpStatus.SC_OK) {
			result = true;
		}

		log.info("Fedora client connection status OK: " + result);

		return result;
	}

	public void initClientConnection() throws Exception {

		initClient();
		initFedoraAPI();
		initUploader();

	}

	private boolean initFedoraAPI() throws Exception {
		boolean result = true;
		Exception exception = null;
		try {
			if ((client != null) && canConnect()) {

				if (APIA == null) {
					APIA = client.getAPIA();
				}

				if (APIM == null) {
					APIM = client.getAPIM();
				}

			} else {
				result = false;
				exception = new KAPServiceException(
						"Cannot instantiate Fedora API"
								+ ServiceErrorCode.FEDORA_CLIENT_EMPTY
										.getValue(),
						ServiceErrorCode.FEDORA_CLIENT_EMPTY.getValue());
			}
		} catch (Exception e) {
			result = false;
			exception = new KAPServiceException(e.getMessage(),
					ServiceErrorCode.FEDORA_API_ERROR.getValue());
		} finally {
			if (exception != null) {
				throw exception;
			} else {
				log.info("Fedora API has been initialized.");
			}
		}
		return result;
	}

	public void initUploader() throws KAPServiceException {
		try {
			log.info("Initializing Uploader: " + "Host: "
					+ KAPService.getHost() + " Port: " + KAPService.getPort());
			this.uploader = new Uploader("http", KAPService.getHost(),
					KAPService.getPort(), KAPService.getUser(),
					KAPService.getPassword());
		} catch (IOException e) {
			throw new KAPServiceException(e.getMessage(),
					ServiceErrorCode.FEDORA_UPLOADER_ERROR.getValue());
		}
	}

	public static String getConnection() {

		log.debug("Connection: Fedora Host: " + KAPService.getHost()
				+ " Fedora Port: " + KAPService.getPort());
		String connection = "http://" + KAPService.getHost() + ":"
				+ KAPService.getPort() + "/fedora";

		return connection;
	}

	public ExecutorService getConnectionPool() {
		return connectionPool;
	}

	public void shutdownPools() {

		connectionPool.shutdownNow();

		if (connectionPool.isTerminated()) {
			log.info("Connection Pool has been shutdown:"
					+ connectionPool.toString());
		} else {
			log.info("Connection Pool has not been shutdown!!!:"
					+ connectionPool.toString());
		}

	}

	/* End Fedora Connection Methods */

	private void validateClientConnection() throws InterruptedException,
			ExecutionException {
		Future connectionTask = getConnectionPool().submit((new Runnable() {

			@Override
			public void run() {
				connectionTimer.reset();
				connectionTimer.start();
				boolean isValidConnection = false;

				if (!canConnect()) {
					while (!isValidConnection) {
						try {
							log.info("Trying reconnect to Fedora ...");
							initClientConnection();
						} catch (IOException e) {
							log.error("Error while reconnecting to Fedora Client. "
									+ getConnection());
						} catch (KAPServiceException e) {
							log.error("Error while reconnecting to Fedora Client. "
									+ getConnection());
						} catch (Exception e) {
							log.error("Error while reconnecting to Fedora Client. "
									+ getConnection());
						}
						isValidConnection = canConnect();
					}
				}
				connectionTimer.stop();
			}
		}));

		while (true) {
			if (connectionTask.isDone()) {
				connectionTask.get();
				log.info("Client connection has been validated successfully. Total time taken. "
						+ connectionTimer.toString());
				break;
			}
			if (!connectionTask.isDone()) {
				connectionTask.get();
				log.info("Validating client connection...");
			}
		}
	}

	private void getZoomify(final Future<String> task, final String pid) {

		while (true) {
			if (task.isDone()) {

				log.info("Content has been zoomified." + "UMAM PID : = " + pid);
				break;
			}
			if (!task.isDone()) {
				try {
					task.get();
				} catch (InterruptedException e) {
					log.error("Error getting Zoomify");
				} catch (ExecutionException e) {
					log.error("Error getting Zoomify");
					e.printStackTrace();
				}

				log.info("Waiting for content being zoomified. "
						+ "UMAM PID : = " + pid + "Content has been zoomified."
						+ "UMAM PID : = " + task);
			}

			completedZoomifyTasks++;

			log.info("# of Completed Zoomified Tasks: " + completedZoomifyTasks);
		}
	}

	private UMAMContentObject getUMAMContent(Future<UMAMContentObject> task,
			final String umamId) {

		UMAMContentObject umamContent = new UMAMContentObject();

		while (true) {
			if (task.isDone()) {

				log.info("Content has been loaded." + "UMAM ID : = " + umamId);
				break;
			}
			if (!task.isDone()) {
				try {
					umamContent = task.get();
				} catch (InterruptedException e) {
					log.error("Error uploading item");
				} catch (ExecutionException e) {
					log.error("Error uploading item");
					e.printStackTrace();
				}

				log.info("Waiting for content being loaded. UMAM ID : = "
						+ umamId);
			}

		}
		return umamContent;
	}

}
