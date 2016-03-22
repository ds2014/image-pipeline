package edu.umd.lims.fedora.kap;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;

public class UtilsProperties {

	public static final Logger log = LoggerFactory
			.getLogger(UtilsProperties.class);

	private static final UtilsProperties INSTANCE = new UtilsProperties();
	private static Properties properties;
	private final String propertyFileName = ".umfedora-properties";

	/**
	 * Default constructor
	 */
	private UtilsProperties() {
		properties = retrieveProperties();
	}

	private Properties retrieveProperties() {
		Properties p = new Properties();

		// Set default values
		p.setProperty("fedora.host", "fedoradev.lib.umd.edu");
		p.setProperty("fedora.port", "80");
		p.setProperty("fedora.user", "fedoraAdmin");
		p.setProperty("fedora.password", "0b1kn0b");
		p.setProperty("targetStatus", "Pending");
		p.setProperty("maxrecords", "3");
		p.setProperty("batch.size", "5");
		p.setProperty("data.path", "/apps/KAP/data");
		p.setProperty("master.path", "/apps/KAP/master/metadata-master.csv");
		p.setProperty("master.staged.path", "/apps/KAP/staged/master-staged.csv");
		p.setProperty("master.processed.path", "/apps/KAP/processed/master-processed.csv");
		p.setProperty("output.processed.path", "/apps/KAP/master/output-processed.csv");
		p.setProperty("errors.log.path", "/apps/KAP/logs/errors.csv");
		p.setProperty("logs.path", "/apps/KAP/logs");
		p.setProperty("log4j.path", "/apps/KAP/conf");
		
		p.setProperty("staged.partitions.path", "/apps/KAP/staged");
		p.setProperty("processed.partitions.path", "/apps/KAP/processed");
		p.setProperty("processed.errors.path", "/apps/KAP/logs");
		
		p.setProperty("partition.folder", "");
		
		p.setProperty("sample.probe.size", "2");
				

		// Override with default properties files
		FileInputStream fileStream = null;

		try {
			File home = new File(System.getProperty("user.home"));
			File f = new File(home, propertyFileName);

			if (!f.exists()) {
				// The properties file doesn't exist, use the default properties
				log.info("Use the default properties");
				return p;
			}

			fileStream = new FileInputStream(f);

			if (f.canRead()) {
				p.load(fileStream);
				log.info("Use the properties from " + home + "/"
						+ propertyFileName);
			}
			fileStream.close();

		} catch (IOException e) {
			log.warn("Error to access the property file ~/" + propertyFileName
					+ " and use default properties instead: " + e.getMessage());
		} catch (IllegalArgumentException iae) {
			log.warn("Syntax error in the property file ~/" + propertyFileName
					+ "and use default properties instead: " + iae.getMessage());
		}

		return p;
	}

	

	/**
	 * Get the singleton of the class
	 * 
	 * @return
	 */
	public static UtilsProperties getInstance() {
		return INSTANCE;
	}

	/**
	 * Get the Properties object
	 * 
	 * @return
	 */
	public static Properties getProperties() {
		return properties;
	}

}
