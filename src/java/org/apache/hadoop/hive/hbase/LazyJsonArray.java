package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.json.JSONArray;
import org.json.JSONException;

public class LazyJsonArray extends LazyArray {
	
	List<Object> cachedList ;
	JSONArray jr;
	boolean parsed = false;

	protected LazyJsonArray(LazyListObjectInspector oi) {
		super(oi);
	}
	
	@Override
	public void init(ByteArrayRef bytes, int start, int length) {
	    // do nothing
	}
	
	private void parse() {
		if (cachedList == null) {
			cachedList = new ArrayList<Object>();
		} else {
			cachedList.clear();
		}
		parsed = true;
		if(jr == null)
			return;
		try {
			int size = jr.length();
			for (int i = 0; i < jr.length(); i++) {
				LazyObject ob = LazyFactory
						.createLazyObject(((ListObjectInspector) getInspector())
								.getListElementObjectInspector());
				String data = jr.getString(i);
				ByteArrayRef obRef = new ByteArrayRef();
				obRef.setData(data.getBytes());
				ob.init(obRef, 0, data.length());
				cachedList.add(ob.getObject());
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}

	}
	
	  public void init(JSONArray jsonlist) {
		  jr = jsonlist;
		  parsed = false;
		  cachedList  = null;// jsonlist;
	  }
	  
	  @Override
	public Object getListElementObject(int index) {
		  
		if(!parsed)
			parse();
		return cachedList.get(index);
	}
	  
	  @Override
	public int getListLength() {
		  if(!parsed)
			  parse();
		return cachedList.size();
	}
	  
	  @Override
	public List<Object> getList() {
		  if(!parsed)
			  parse();
		return cachedList;
 	}
	  
	

}
