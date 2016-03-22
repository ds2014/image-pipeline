package edu.umd.lims.fedora.kap;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

public class UMDMCSVInput {

	private String id;
	private int lineNumber;
	private String type;
	private String collection;
	private String series;
	private String subseries;
	private String box;
	private String folder;
	private String item;
	private String title;
	private String date;
	private String handle;
	private String accession;
	private String size;
	private String fileName;
	private String label;
	private String rank;
	private String creator;
	private String continent;
	private String country;
	private String region;
	private String settlement;
	private String pid;

	private TreeMap<String, UMAMCSVInput> childUMAM = new TreeMap<String, UMAMCSVInput>();

	public UMDMCSVInput() {
		childUMAM = new TreeMap<String, UMAMCSVInput>();
	}

	public UMDMCSVInput(String id, int lineNumber, String type,
			String collection, String series, String subseries, String box,
			String folder, String item, String title, String date,
			String handle, String accession, String size, String fileName, String label,
			String rank, String creator, String continent, String country,
			String region, String settlement) {
		this.id = id;
		this.lineNumber = lineNumber;
		this.type = type;
		this.collection = collection;
		this.series = series;
		this.subseries = subseries;
		this.box = box;
		this.folder = folder;
		this.item = item;
		this.title = title;
		this.date = date;
		this.handle = handle;
		this.accession = accession;
		this.size = size;
		this.fileName = fileName;
		this.label = label;
		this.rank = rank;
		this.creator = creator;
		this.continent = continent;
		this.country = country;
		this.region = region;
		this.settlement = settlement;
	}

	public UMDMCSVInput(String fileName) {
		this.fileName = fileName;
	}

	public UMDMCSVInput(String id, String fileName) {
		this.id = id;
		this.fileName = fileName;
	}

	public UMDMCSVInput(String id, String fileName, String label, String rank) {
		this.id = id;
		this.fileName = fileName;
		this.label = label;
		this.rank = rank;
	}

	public UMDMCSVInput(String id, String fileName, String label, String rank,
			String type) {
		this.id = id;
		this.fileName = fileName;
		this.label = label;
		this.rank = rank;
		this.type = type;
	}

	public int getLineNumber() {
		return lineNumber;
	}

	public void setLineNumber(int lineNumber) {
		this.lineNumber = lineNumber;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public String getSeries() {
		return series;
	}

	public void setSeries(String series) {
		this.series = series;
	}

	public String getSubseries() {
		return subseries;
	}

	public void setSubseries(String subseries) {
		this.subseries = subseries;
	}

	public String getBox() {
		return box;
	}

	public void setBox(String box) {
		this.box = box;
	}

	public String getFolder() {
		return folder;
	}

	public void setFolder(String folder) {
		this.folder = folder;
	}

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getRank() {
		return rank;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}

	public String getCreator() {
		return creator;
	}

	public void setCreator(String creator) {
		this.creator = creator;
	}

	public String getContinent() {
		return continent;
	}

	public void setContinent(String continent) {
		this.continent = continent;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getSettlement() {
		return settlement;
	}

	public void setSettlement(String settlement) {
		this.settlement = settlement;
	}

	public String getId() {
		return this.id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public TreeMap<String, UMAMCSVInput> getChildUMAM() {
		return this.childUMAM;
	}

	public String getHandle() {
		return this.handle;
	}

	private void setHandle(String handle) {
		this.handle = handle;
	}

	public String getAccession() {
		return accession;
	}

	public void setAccession(String accession) {
		this.accession = accession;
	}

	public void setChildUMAM(TreeMap<String, UMAMCSVInput> childUMAM) {
		this.childUMAM = childUMAM;
	}

	public String toString() {

		StringBuilder objectName = new StringBuilder("UMDM Object : "
				+ this.getId());

		if (this.getType() != null) {
			objectName = objectName.append("\n").append("; Type = ")
					.append(this.getType());
		}

		if (this.getCollection() != null) {
			objectName = objectName.append("\n").append("; Collection = ")
					.append(this.getCollection());
		}

		if (this.getSeries() != null) {
			objectName = objectName.append("\n").append("; Series = ")
					.append(this.getSeries());
		}

		if (this.getSubseries() != null) {
			objectName = objectName.append("\n").append("; Subseries = ")
					.append(this.getSubseries());
		}

		if (this.getBox() != null) {
			objectName = objectName.append("\n").append("; Box = ")
					.append(this.getBox());
		}

		if (this.getFolder() != null) {
			objectName = objectName.append("\n").append("; Folder = ")
					.append(this.getFolder());
		}

		if (this.getItem() != null) {
			objectName = objectName.append("\n").append("; Item = ")
					.append(this.getItem());
		}

		if (this.getTitle() != null) {
			objectName = objectName.append("\n").append("; Title = ")
					.append(this.getTitle());
		}

		if (this.getDate() != null) {
			objectName = objectName.append("\n").append("; Date = ")
					.append(this.getDate());
		}

		if (this.getHandle() != null) {
			objectName = objectName.append("\n").append("; Handle = ")
					.append(this.getHandle());
		}

		if (this.getSize() != null) {
			objectName = objectName.append("\n").append("; Size = ")
					.append(this.getSize());
		}

		if (this.getFileName() != null) {
			objectName = objectName.append("\n").append("; FileName = ")
					.append(this.getFileName());
		}

		if (this.getLabel() != null) {
			objectName = objectName.append("\n").append("; Label = ")
					.append(this.getLabel());
		}

		if (this.getRank() != null) {
			objectName = objectName.append("\n").append("; Rank = ")
					.append(this.getRank());
		}

		if (this.getCreator() != null) {
			objectName = objectName.append("\n").append("; Creator = ")
					.append(this.getCreator());
		}

		if (this.getContinent() != null) {
			objectName = objectName.append("\n").append("; Continent = ")
					.append(this.getContinent());
		}

		if (this.getCountry() != null) {
			objectName = objectName.append("\n").append("; Country = ")
					.append(this.getCountry());
		}

		if (this.getRegion() != null) {
			objectName = objectName.append("\n").append("; Region = ")
					.append(this.getRegion());
		}

		if (this.getSettlement() != null) {
			objectName = objectName.append("\n").append("; Settlement = ")
					.append(this.getSettlement());
		}

		if (this.childUMAM != null) {
			objectName = objectName.append("\n").append("; UMAM Count = ")
					.append(this.childUMAM.size());

			for (Entry<String, UMAMCSVInput> item : childUMAM.entrySet()) {

				String page = item.getValue().toString();

				objectName = objectName.append("\n").append("; UMAM = ")
						.append(page);
			}

		}

		if (objectName != null) {
			return objectName.toString();
		} else {
			return null;
		}

	}

	public String getMainTitle() {

		StringBuilder objectName = new StringBuilder();

		if (this.getCollection() != null) {
			objectName = objectName.append(this.getCollection());
		}

		if (this.getSeries() != null) {
			objectName = objectName.append(", ").append("Series ")
					.append(this.getSeries());
		}

		if (this.getSubseries() != null) {
			objectName = objectName.append(", ").append("Subseries ")
					.append(this.getSubseries());
		}

		if (this.getBox() != null) {
			objectName = objectName.append(", ").append("Box ")
					.append(this.getBox());
		}

		if (this.getFolder() != null) {
			objectName = objectName.append(", ").append("Folder ")
					.append(this.getFolder());
		}

		if (this.getItem() != null) {
			objectName = objectName.append(", ").append("Item ")
					.append(this.getItem());
		}

		if (this.getTitle() != null) {
			objectName = objectName.append(", ").append(this.getTitle());
		}

		if (objectName != null) {
			return objectName.toString();
		} else {
			return null;
		}

	}

	public String getDescription() {

		StringBuilder objectName = new StringBuilder(getMainTitle());

		if ( getHandle()!= null && getHandle().length() > 0 ) {

			if (objectName.length() > 0) {
				objectName = objectName.append(". ");
			}

			objectName = objectName.append(getHandleString());
			
			objectName = objectName.append(" ").append(getHandle()).append(".");
		}

		if (objectName != null) {
			return objectName.toString();
		} else {
			return "";
		}
	}

	private static String getHandleString() {

		String result = "For complete collection information, visit the full finding aid at";
		return result;

	}

	public static String getCopyRightOwner() {
		String result = "Katherine Anne Porter Literary Trust";
		return result;
	}

	public static String getRights() {
		String result = "Collection protected under Title 17 of the U.S. Copyright Law. Inquiries aboout permission to publish or reproduce the intellectual property of Katherine Anne Porter should be directed to the Permissions Company, Inc., 47 Seneca Road, P.O. Box 604, Mount Pocono, PA 18344. Phone: 570-839-7477. Fax: 570-839-7448. Email: permdude@eclipse.net.";
		return result;
	}
	
	public String getPid(){
		return this.pid;
	}
	
	public void setPid(String pid){
		this.pid = pid;
	}
	
}
