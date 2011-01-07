package org.apache.hadoop.hive.hbase.runa;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.hbase.LazyHBaseCellMap;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class LazyRunaHbaseRow extends LazyStruct {

	  private List<String> hbaseColumns;
	  private RowResult rowResult;
	  private ArrayList<Object> cachedList;

	
	public LazyRunaHbaseRow(LazySimpleStructObjectInspector oi) {
		super(oi);
	}
	
	 public void init(RowResult rr, List<String> hbaseColumnNames) {
		    this.rowResult = rr;
		    this.hbaseColumns = hbaseColumnNames;
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
		        String hbaseColumn = hbaseColumns.get(i);
		        if (hbaseColumn.endsWith(":")) {
		          // a column family
		          getFields()[i] = 
		            new LazyHBaseCellMap(
		              (LazyMapObjectInspector)
		              fieldRefs.get(i).getFieldObjectInspector());
		          continue;
		        }
		        
		        getFields()[i] = LazyFactory.createLazyObject(
		          fieldRefs.get(i).getFieldObjectInspector());
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
		      Set<byte[]> allKeys = rowResult.keySet();
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
		        	if(rowResult.containsKey(newColumnName)) {
		        		String val = JsonColumnParser.parseJsonInField(columnName,new String(rowResult.get(newColumnName).getValue()));
		        		ref = new ByteArrayRef();
		        		ref.setData(val.getBytes());
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


}
