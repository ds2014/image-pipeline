package edu.umd.lims.fedora.kap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

public class ValidationService {

	private static final Logger log = LoggerFactory
			.getLogger(ValidationService.class);
	private final static StopWatch timer = new StopWatch();

	private static boolean debug = false;
	private static File log4jConfig;
	private static String sBatchID = "";
	private static int lineNumber = 0;
	private static int umdmLineNumber = 0;
	private static int errorCount = 0;

	public static void main(String[] args) {
		init(args);

	}

	private static void init(String[] args) {

		Exception exception = null;
		InputStream inputStream = null;

		try {
			parseCommandLine(args);

			// Setup logging
			if (log4jConfig != null) {
				log.debug(log4jConfig.getAbsolutePath());
				PropertyConfigurator.configure(log4jConfig.getAbsolutePath());
			} else {
				inputStream = Thread
						.currentThread()
						.getContextClassLoader()
						.getResourceAsStream(
								"edu/umd/lims/fedora/kap/util/log4j.conf");

				Properties pLog4j = new Properties();
				pLog4j.load(inputStream);
				PropertyConfigurator.configure(pLog4j);
			}

			if (debug) {
				org.apache.log4j.Logger.getLogger(ValidationService.class)
						.setLevel(Level.DEBUG);
			} else {
				org.apache.log4j.Logger.getLogger(ValidationService.class)
						.setLevel(Level.INFO);
			}

			Properties p = UtilsProperties.getProperties();

			ValidationService.validate();

		} catch (KAPServiceException e) {
			exception = e;
		} catch (ParseException e) {
			exception = e;
		} catch (IOException e) {
			exception = e;
		} catch (Exception e) {
			exception = e;
		} finally {
			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {
				log.info("Logj4 properties inputStream is null. Skipping closing of inputstream");
			}

			if (exception != null) {
				log.error("Exception occurred during Validation. "
						+ " Last Line Number: " + ValidationService.lineNumber
						+ " - " + exception.getMessage());

				// if (exception instanceof KAPServiceException){
				// KAPServiceException serviceException = (KAPServiceException)
				// exception;
				// log.error(serviceException.getErrorCode());
				// }
			}

		}
	}

	private static void parseCommandLine(String[] args) throws ParseException {
		// Setup the options
		Options options = new Options();

		Option option = new Option("l", "log4j-config", true,
				"log4j properties file");
		option.setRequired(false);
		options.addOption(option);

		option = new Option("d", "debug", false, "debug logging");
		option.setRequired(false);
		options.addOption(option);

		option = new Option("h", "help", false, "get this list");
		option.setRequired(false);
		options.addOption(option);

		// Parse the command line
		if (args.length == 1
				&& (args[0].equals("-h") || args[0].equals("--help"))) {
			printUsage(options);
		}

		PosixParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		// Handle results
		if (cmd.hasOption('h')) {
			printUsage(options);
		}

		if (cmd.hasOption('d')) {
			debug = true;
		}

		if (cmd.hasOption('l')) {
			log4jConfig = new File(cmd.getOptionValue('l'));
			if (!log4jConfig.canRead()) {
				printUsage(options,
						"Unable to open " + log4jConfig.getAbsoluteFile()
								+ " for reading");
			}
		}

	}

	public static void validate(String sUseThisBatchID)
			throws KAPServiceException {
		sBatchID = sUseThisBatchID;
		validate();
	}

	public static void validate() throws KAPServiceException {

		timer.reset();
		timer.start();
		log.info("Metadata validation has started.");

		Exception exception = null;
		try {
			// log errors here in a loop

			log.debug("Validate!");

			// First lets find out what we need to validate
			// Get the properties file so we can check things out
			Properties p = UtilsProperties.getProperties();

			// Input file
			String strDataDirectory = p.getProperty("data.path");
			String strLogDirectory = p.getProperty("logs.path");

			// Open and initialize the log file
			if (sBatchID == null || sBatchID.length() < 1) {

				Date date = new Date();
				SimpleDateFormat sdf = new SimpleDateFormat(
						"MM-dd-yyyy-h-mm-ss-a");
				sBatchID = sdf.format(date);
			}

			FileWriter fValFile = new FileWriter(strLogDirectory
					+ "/validInput-" + sBatchID + ".log");

			validateMaster(p, fValFile);

			fValFile.close();

		} catch (Exception e) {
			exception = e;
		} finally {

			timer.stop();
			log.info("Dataset validation has been completed. Lines Processed: "
					+ lineNumber + ". Total time taken." + timer.toString());

			if (exception != null) {
				String errorMessage = "Critical errors have been found while validating dataset . Refer to the log file location."
						+ "\n" + exception.getMessage();
				log.error("Critical errors have been found while validating dataset . Refer");
				// exception.printStackTrace();
				throw new KAPServiceException(errorMessage,
						ServiceErrorCode.UMAM_MD_ERROR.getValue());

			} else {
				log.info("Dataset validation has been completed successfully.");
			}
		}

		// some summary stats can be go here.
	}

	private static void validateMaster(Properties p, FileWriter fValFile) {

		ICsvMapReader mapReader = null;
		String strMasterFile = p.getProperty("master.path").trim();
		String contentType = "";
		String strExceptionMsg = "";
		// int lineNumber = 0;
		// int umdmLineNumber = 0;
		boolean bJpeg = false;
		boolean bOcr = false;
		boolean bHocr = false;
		boolean bTiff = false;
		boolean bPdf = false;

		log.debug("Reading metadata input file: >>" + strMasterFile + "<<");

		try {
			FileReader fileReader = new FileReader(strMasterFile);

			fValFile.write("Processing " + strMasterFile + "\n");
			fValFile.flush();

			mapReader = new CsvMapReader(fileReader,
					CsvPreference.STANDARD_PREFERENCE);

			String[] header = mapReader.getHeader(true);
			String badColumns = checkHeader(header);

			// Check out the header
			if (badColumns.length() > 0) {
				strExceptionMsg = "Line " + lineNumber + ": Bad columns: "
						+ badColumns;
			}

			final CellProcessor[] processors = getProcessors();
			Map<String, Object> kapMap;
			boolean bReadMore = true;

			while ((bReadMore) && (strExceptionMsg.length() < 1)) {

				try {

					if ((kapMap = mapReader.read(header, processors)) == null) {
						bReadMore = false;
					} else {

						Map<String, Object> processedRow = preProcessRow(kapMap);
						lineNumber = mapReader.getLineNumber();

						// Test XML Type
						// Rule - XML Type must be UMDM or UMAM
						contentType = (String) processedRow.get("XML Type");

						if (contentType.equalsIgnoreCase(ContentType.UMDM
								.toString())) {

							umdmLineNumber = lineNumber;

						} else if (contentType
								.equalsIgnoreCase(ContentType.UMAM.toString())) {

							// Rule - UMAMs must be preceded by a UMDM
							if (umdmLineNumber == 0) {
								strExceptionMsg = "ERROR: Line " + lineNumber
										+ ": UMAMs must be preceded by a UMDM";
								break;
							}

						} else {
							strExceptionMsg = "ERROR: Line " + lineNumber
									+ " XML Type - must be either UMDM or UMAM";
							break;
						}
						// End XML Type test

						// Files test
						String fileName = (String) processedRow.get("filename");

						String objectId = getObjectId(contentType, fileName);

						log.debug("Searching for object by file pattern: "
								+ objectId);

						Path rootPath = Paths.get(p.getProperty("data.path"));

						File root = rootPath.toFile();

						TreeMap<String, String> files = new TreeMap<String, String>();

						FileProcessor.searchForUMAMFiles(root, files, objectId);

						int fileCount = 0;

						for (Entry<String, String> file : files.entrySet()) {

							String filePath = file.getValue().toString();
							String fileExtension = FilenameUtils
									.getExtension(filePath);

							if (fileExtension.equals("jpg")) {
								bJpeg = true;
								;
							} else if (fileExtension.equals("txt")) {
								bOcr = true;
								;
							} else if (fileExtension.equals("xml")) {
								bHocr = true;
								;
							} else if (fileExtension.equals("tiff")) {
								bTiff = true;
							} else if (fileExtension.equals("pdf")) {
								bPdf = true;
							}
							// End file tests

							fileCount++;

						}

						// Rule - Everything must have a jpeg here
						if (!bJpeg) {
							// strExceptionMsg = "ERROR: Line " + lineNumber +
							// ": "
							// + contentType + " does not have a JPEG";

							if (contentType.equalsIgnoreCase(ContentType.UMDM
									.toString())) {

								fValFile.write("ERROR: Line " + lineNumber
										+ ": " + contentType + " - File Name:  "
										+ fileName + " (" + objectId + ")" + " does not have a JPEG "
										+ " Files Found: " + fileCount + "\n");
								fValFile.flush();
								errorCount++;

								log.error("ERROR: Line " + lineNumber + ": "
										+ contentType + " File Name:  "
										+ fileName + " (" + objectId + ")" + " does not have a JPEG ");

							}

							// break;
						}

						if (contentType.equalsIgnoreCase(ContentType.UMAM
								.toString())) {

							// Warning both ocr and hocr should be present or
							// absent
							if ((bOcr && !bHocr) || (!bOcr && bHocr)) {
								fValFile.write("Warning: Line "
										+ lineNumber
										+ ": OCR and hOCR are not both present\n");
								fValFile.flush();
							}

						}

						// reset the booleans
						bJpeg = false;
						bOcr = false;
						bHocr = false;
						bTiff = false;
						bPdf = false;

						// Rule: If there is only one value then it must be
						// YYYY[-MM[-DD]] or undated
						// Rule: Here are the position rules
						// First may be YYYY[-MM[-DD]], circa, before, after, or
						// undated
						// Second may be YYYY[-MM[-DD]]
						// Third must be YYYY[-MM[-DD]]
						String baseDate = (String) processedRow.get("date");
						;
						String[] dateParts = baseDate.split(" ");
						String certainty = "circa";
						String fromDate = null;
						String toDate = null;
						String dateElement = "date";
						String strDateErrors = "Date format Errors";
						boolean bDateErrors = true;

						if (dateParts.length > 1) {
							if (dateParts[0].equalsIgnoreCase("undated")) {
								certainty = "unknown";
							} else if (dateParts[0].equalsIgnoreCase("circa")) {
								if (checkDate(dateParts[1])) {
									fromDate = dateParts[1];
								} else {
									strDateErrors += "|from date is not recognized|";
									bDateErrors = false;
								}
								if (dateParts.length > 2) {
									if (checkDate(dateParts[2])) {
										toDate = dateParts[2];
									} else {
										strDateErrors += "|to date is not recognized|";
										bDateErrors = false;
									}
								}
							} else if (dateParts[0].equalsIgnoreCase("before")) {
								fromDate = "1920";
								if (checkDate(dateParts[1])) {
									toDate = dateParts[1];
								} else {
									strDateErrors += "|to date is not recognized|";
									bDateErrors = false;
								}
							} else if (dateParts[0].equalsIgnoreCase("after")) {
								if (checkDate(dateParts[1])) {
									fromDate = dateParts[1];
								} else {
									strDateErrors += "|from date is not recognized|";
									bDateErrors = false;
								}
								toDate = "1980";
							} else if (checkDate(dateParts[0])) {
								certainty = "exact";
								fromDate = dateParts[0];
								if (checkDate(dateParts[1])) {
									toDate = dateParts[1];
								} else {
									strDateErrors += "|to date is not recognized|";
									bDateErrors = false;
								}
							} else {
								strDateErrors += "|from date is not recognized|";
								bDateErrors = false;
							}
						} else {
							if (dateParts[0].equalsIgnoreCase("undated")) {
								certainty = "unknown";
							} else {
								certainty = "exact";
								if (checkDate(dateParts[0])) {
									fromDate = dateParts[0];
								} else {
									strDateErrors += "|date is not recognized|";
									bDateErrors = false;
								}
							}
						}

						if (!bDateErrors) {
							fValFile.write("Error: Line " + lineNumber + ": "
									+ strDateErrors + "\n");
							fValFile.flush();
							errorCount++;
						}
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();

					errorCount++;

					log.error("Exception occurred reading a data row. "
							+ " Last Line Number Processed: "
							+ ValidationService.lineNumber + " - "
							+ e.getMessage());
				}

			}

			mapReader.close();

			if ((strExceptionMsg.length() > 0) || (errorCount > 0)) {

				if (strExceptionMsg.length() == 0) {
					strExceptionMsg = "Encountered " + errorCount
							+ " format errors.";
				}
				fValFile.write(strExceptionMsg + "\n");
				fValFile.flush();
				throw new KAPServiceException(strExceptionMsg,
						"Validation Exception");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			log.error("File not found Exception - " + e.getMessage());
		}

	}

	private static CellProcessor[] getProcessors() {

		// apply some constraints to ignored columns (just because we can)
		final CellProcessor[] processors = new CellProcessor[] { new NotNull(), // XML
																				// Type
				new Optional(), // Collection
				new Optional(), // Series
				new Optional(), // SubSeries
				new Optional(), // Box
				new Optional(), // Folder
				new Optional(), // Item
				new NotNull(), // Title
				new Optional(), // Date
				new Optional(), // Size
				null, // Restricted
				new Optional(), // handle
				new Optional(), // accession #
				null, // Handwritten or typed?
				null, // handwritten or typed controlled
				null, // Onionskin?
				null, // Column
				new NotNull(), // fileName
				new NotNull(), // label
				new NotNull(), // rank
				new Optional(), // creator
				new Optional(), // continent
				new Optional(), // country
				new Optional(), // region
				new Optional(), // settlement
				null, // umdm pid
				null, // umam pid
				null, // TEI
		};

		return processors;
	}

	private static Map<String, Object> preProcessRow(Map<String, Object> map) {

		Map<String, Object> items = map;

		for (Entry<String, Object> item : items.entrySet()) {
			String key = item.getKey();
			String value = (String) item.getValue();

			if (value != null) {
				value.trim();
				items.put(key, value);
			}
		}

		return items;
	}

	private static String[] getHeader() {

		final String[] header = new String[] { "XML Type", "collection",
				"series", "subseries", "box", "folder", "item", "title",
				"date", "size", null, "handle", "Accession Number", null, null,
				null, null, "filename", "label", "rank", "creator",
				"continent", "country", "region", "settlement", null, null,
				null };

		return header;
	}

	private static String checkHeader(String[] candidateHeader) {

		String[] officialHeader = getHeader();
		String comparator;
		String badColumns = "";

		log.debug("Checking Header");

		for (int i = 0; i < officialHeader.length; i++) {
			comparator = officialHeader[i];
			if (comparator != null && !comparator.equals(candidateHeader[i])) {
				if (badColumns.length() > 0) {
					badColumns += " ";
				}
				badColumns += candidateHeader[i];
			}
		}
		return badColumns;
	}

	private static boolean checkDate(String strDate) {
		boolean bResult = false;

		if (strDate.matches("[0-9]{4}(-[0-9]{2})?(-[0-9]{2})?")) {
			bResult = true;
		}

		return bResult;

	}

	private static String getObjectId(String contentType, String fileName) {

		String result = null;

		if (contentType.equals(ContentType.UMAM.toString())) {

			result = fileName.trim();

		} else if (contentType.equals(ContentType.UMDM.toString())) {
			int index = fileName.lastIndexOf("-");
			result = fileName.substring(0, index);

		}

		return result;

	}

	public static void printUsage(Options options, String... msgs) {
		PrintWriter err = new PrintWriter(System.err, true);

		// print messages
		for (String msg : msgs) {
			err.println(msg);
		}
		if (msgs.length != 0) {
			err.println();
		}

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(err, 80, "KAPLoader [-l log4j] [-d]", null,
				options, 2, 2, null);

		err.close();

		System.exit(1);
	}
}
