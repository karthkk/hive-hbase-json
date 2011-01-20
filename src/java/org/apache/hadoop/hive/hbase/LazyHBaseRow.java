/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyListObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * LazyObject for storing an HBase row.  The field of an HBase row can be
 * primitive or non-primitive.
 */
public class LazyHBaseRow extends LazyStruct {
	


  
  /**
   * The HBase columns mapping of the row.
   */
  private List<String> hbaseColumns;
  private RowResult rowResult;
  private ArrayList<Object> cachedList;
  
  
  /**
   * Construct a LazyHBaseRow object with the ObjectInspector.
   */
  public LazyHBaseRow(LazySimpleStructObjectInspector oi) {
    super(oi);
  }
  
  /**
   * Set the hbase row data(a RowResult writable) for this LazyStruct.
   * @see LazyHBaseRow#init(RowResult)
   */
  public void init(RowResult rr, List<String> hbaseColumns) {
    this.rowResult = rr;
    this.hbaseColumns = hbaseColumns;
    setParsed(false);
  }

  /**
   * Parse the RowResult and fill each field.
   * @see LazyStruct#parse()
   */
  private void parse() {
	  
    if (getFields() == null) {
      List<? extends StructField> fieldRefs =
        ((StructObjectInspector)getInspector()).getAllStructFieldRefs();
      setFields(new LazyObject[fieldRefs.size()]);
      for (int i = 0; i < getFields().length; i++) {
    	  ObjectInspector fieldObjectInspector = fieldRefs.get(i).getFieldObjectInspector();
        String hbaseColumn = hbaseColumns.get(i);
        if (hbaseColumn.endsWith(":")) {
          // a column family
          getFields()[i] = 
            new LazyHBaseCellMap(
              (LazyMapObjectInspector)
              fieldObjectInspector);
          continue;
        }
        if(fieldObjectInspector instanceof LazyListObjectInspector) {
        	getFields()[i] = new LazyJsonArray((LazyListObjectInspector)fieldObjectInspector);
        	continue;
        }
        
        getFields()[i] = LazyFactory.createLazyObject(fieldObjectInspector);
      }
      setFieldInited(new boolean[getFields().length]);
    }
    Arrays.fill(getFieldInited(), false);
    setParsed(true);
  }
  
  /**
   * Get one field out of the hbase row.
   * 
   * If the field is a primitive field, return the actual object.
   * Otherwise return the LazyObject.  This is because PrimitiveObjectInspector
   * does not have control over the object used by the user - the user simply
   * directly uses the Object instead of going through 
   * Object PrimitiveObjectInspector.get(Object).  
   * 
   * @param fieldID  The field ID
   * @return         The field as a LazyObject
   */
  public Object getField(int fieldID) {
    if (!getParsed()) {
      parse();
    }
    return uncheckedGetField(fieldID);
  }
  
  /**
   * Get the field out of the row without checking whether parsing is needed.
   * This is called by both getField and getFieldsAsList.
   * @param fieldID  The id of the field starting from 0.
   * @param nullSequence  The sequence representing NULL value.
   * @return  The value of the field
   */
  private Object uncheckedGetField(int fieldID) {
	    if (!getFieldInited()[fieldID]) {
	      getFieldInited()[fieldID] = true;
	      
	      ByteArrayRef ref = null;
	      String columnName = hbaseColumns.get(fieldID);
	      if (columnName.equals(HBaseSerDe.HBASE_KEY_COL)) {
	        ref = new ByteArrayRef();
	        ref.setData(rowResult.getRow());
	      } else {
	        if (columnName.endsWith(":")) {
	          // it is a column family
	          ((LazyHBaseCellMap) getFields()[fieldID]).init(
	            rowResult, columnName);
	        } 
	        else if(columnName.endsWith("]")) {
	        	String newColumnName = columnName.split("\\[")[0];
	        	Object val = "";
	        	if(rowResult.containsKey(newColumnName)) {
        			String jsonString = new String(rowResult.get(newColumnName).getValue());
					try {
						JSONObject jso;
						jso = new JSONObject(new JSONTokener(jsonString));
						val = extractFromJson(columnName, jso);
						if(val instanceof JSONArray) {
							((LazyJsonArray)getFields()[fieldID]).init((JSONArray) val);
						} else {
							ref = new ByteArrayRef();
							ref.setData(val.toString().getBytes());
						}

					} catch (JSONException e) {
					}
	        	}
	        } else {
	          // it is a column
	        
	          if (rowResult.containsKey(columnName)) {
	            ref = new ByteArrayRef();
	            ref.setData(rowResult.get(columnName).getValue());
	          } else {
	            return null;
	          }
	        }
	      }
	      if (ref != null) {
	        getFields()[fieldID].init(ref, 0, ref.getData().length);
	      }
	    }
	    return getFields()[fieldID].getObject();
	  }


  /**
   * Get the values of the fields as an ArrayList.
   * @return The values of the fields as an ArrayList.
   */
  public ArrayList<Object> getFieldsAsList() {
	  
    if (!getParsed()) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < getFields().length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }
  
  @Override
  public Object getObject() {
    return this;
  }
  
	private static Pattern keyPatterns = Pattern.compile("\\[(.*?)\\]");
	
	

	private static Object extractFromJson(String columnDefn,
			JSONObject jsonObject) {
		
		Object result = null;
		try {
			List<String> keys = getKeys(columnDefn);
			result = extractData(jsonObject, keys);
		} catch (Exception e) {
		}
		
		return result;
	}

	private static Object extractData(JSONObject jsonObject, List<String> keys)
			throws JSONException {
		Object currentObject = jsonObject;
		for (Iterator<String> iterator = keys.iterator(); iterator.hasNext();) {
			String key = (String) iterator.next();
			if(currentObject!=null && currentObject instanceof JSONObject)
				currentObject = ((JSONObject)currentObject).get(key);
		}
		return currentObject;
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
