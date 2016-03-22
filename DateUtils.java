package edu.umd.lims.fedora.kap;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateUtils {
	
	private static Calendar calendar = Calendar.getInstance();
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static String getFormattedCurrentTime(){
		
		 SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 String result =formatter.format(calendar.getTime());
		 return result;
	}

}
