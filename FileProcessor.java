package edu.umd.lims.fedora.kap;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;

public class FileProcessor {

	private static final Logger log = LoggerFactory
			.getLogger(FileProcessor.class);

	
	
	public static void main(String[] args) throws KAPServiceException {
		Path rootPath = Paths.get("/apps/KAP/data");
		File root = rootPath.toFile();
		System.out.println("Starting Directory:" + root.getAbsolutePath());
		String pattern = "litmss-021682";
		
		List<File> umamFiles = new ArrayList<File>();
		searchForUMAMFiles(root,umamFiles, pattern);
		int fileCount = 0;
		
		for (File file: umamFiles){
			System.out.println("File path: " + file.getAbsolutePath());
			fileCount++;
		}
		
		System.out.println("Found files: " + fileCount);
		
	}
	
	static String getAbsoluteFilePath(Path path) {

		if ((path != null)) {
			return path.toAbsolutePath().toFile().getPath();
		} else {
			log.error("Get Absolute Path: Supplied Path is null. Returning null");
			return null;
		}

	}

	public static void searchForUMAMFiles(File root, List<File> umamFiles,
			String pattern) {
			
		if (root == null || umamFiles == null)
			return;

		if (root.isDirectory()) {
			for (File file : root.listFiles())
				searchForUMAMFiles(file, umamFiles, pattern);
		} else {
			if (root.isFile()) {
				String fileNameNoExtension = FilenameUtils.removeExtension(root
						.getName());
				if (fileNameNoExtension.startsWith(pattern)) {
					umamFiles.add(root);
				}
			}
		}
	}
	

	public static void searchForUMAMFiles(File root, TreeMap<String, String> umamFiles,
			String pattern) {
			
		if (root == null || umamFiles == null)
			return;

		if (root.isDirectory()) {
			for (File file : root.listFiles())
				searchForUMAMFiles(file, umamFiles, pattern);
		} else {
			if (root.isFile()) {
				String fileNameNoExtension = FilenameUtils.removeExtension(root
						.getName());
				if (fileNameNoExtension.startsWith(pattern)) {
					umamFiles.put(root.getName(), root.getAbsolutePath());
				}
			}
		}
	}
}
