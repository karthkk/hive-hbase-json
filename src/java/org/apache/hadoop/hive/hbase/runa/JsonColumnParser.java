package org.apache.hadoop.hive.hbase.runa;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;



public class JsonColumnParser {

	private static Pattern keyPatterns = Pattern.compile("\\[(.*?)\\]");
	
	public static String parseJsonInField(String columnDefn, String json) {
		JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON( json );
		List<String> keys = getKeys(columnDefn);
		JSONObject currentObject = jsonObject;
		String result = null;
		for (Iterator iterator = keys.iterator(); iterator.hasNext();) {
			String key = (String) iterator.next();
			if(iterator.hasNext() && currentObject!=null)
				currentObject = currentObject.getJSONObject(key);
			if(!iterator.hasNext() && currentObject!=null)
				result = currentObject.getString(key);
		}
		return result;
	}

	public static List<String> getKeys(String columnDefn) {
		Matcher m =  keyPatterns.matcher(columnDefn);
		List<String> keys = new ArrayList<String>();
		while(m.find()) {
			keys.add(m.group(1));
		}
		return keys;
	}
}
