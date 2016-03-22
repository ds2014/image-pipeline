package edu.umd.lims.fedora.kap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.time.StopWatch;


public class KAPLoader {

	private static final Logger log = LoggerFactory.getLogger(KAPLoader.class);

	private static boolean debug = false;
	private static File log4jConfig;
	private final static StopWatch KAPLoaderTimer = new StopWatch();
	

	public static void main(String[] args) {
		init(args);

	}

	private static void init(String[] args) {

		Exception exception = null; 
		InputStream inputStream = null;
		
		KAPLoaderTimer.reset();
		KAPLoaderTimer.start();
	    
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
				org.apache.log4j.Logger.getLogger(KAPService.class).setLevel(
						Level.DEBUG);
			} else {
				org.apache.log4j.Logger.getLogger(KAPService.class).setLevel(
						Level.INFO);
			}

			Properties p = UtilsProperties.getProperties();
			
			//ValidationService.validate();
						
	        KAPService.initialize();
	        DatasetProcessor.processDataset();
	                             
	        KAPLoaderTimer.stop();
	        
	        
		} catch (KAPServiceException e){
			exception = e;
		}
		catch (ParseException e) {
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
			
			KAPService.shutdownPools();
			KAPService.closeLogWriters();
			
			if (exception != null) {
		        log.error("Exception occurred during KAP Loading. "
		            + exception.getMessage());
		        
		        if (exception instanceof KAPServiceException){
		        	KAPServiceException serviceException = (KAPServiceException) exception;
		       	log.error(serviceException.getErrorCode());
		        }
		      }
		      
		      log.info("Loading Process Completed. " + " Total time taken. " + KAPLoaderTimer.toString());
		      
		      LoaderStats.getPartitionsSummaryStatistics();
		      
		      log.info(LoaderStats.getStatistics());
		    
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
