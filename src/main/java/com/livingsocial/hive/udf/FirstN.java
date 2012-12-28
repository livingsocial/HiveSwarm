package com.livingsocial.hive.udtf;

import com.livingsocial.hive.utils.KISSInspector;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.*;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

@Description(
    name = "first_n",
    value = "_FUNC_(group_by, values, how_many) - return table of first how_many values by group_by"
)

public class FirstN extends GenericUDTF {
    private class FirstNSelector {                           
	private KISSInspector group_inspector, value_inspector, max_inspector;
	private Object current_group = null;
	private int current_count;

	public FirstNSelector(ObjectInspector gpoi, ObjectInspector vpoi, ObjectInspector maxoi) {
	    group_inspector = new KISSInspector(gpoi);
	    value_inspector = new KISSInspector(vpoi);
	    max_inspector = new KISSInspector(maxoi);
	}
	
	public Object[] getFirstN(Object group, Object value, Object max) { 
	    Object[] result = null;
	    int maxi = (new Float(max_inspector.toFloat(max))).intValue();

	    if(!group_inspector.get(group).equals(current_group)) {
		current_group = group_inspector.get(group);
		current_count = 1;
		result = new Object[] { group_inspector.get(group), value_inspector.get(value) };
	    } else if(current_count < maxi) {
		current_count += 1;
		result = new Object[] { group_inspector.get(group), value_inspector.get(value) };
	    }

	    return result;
	}

	public AbstractPrimitiveJavaObjectInspector getGroupInspector() {
	    return group_inspector.getAnInspector();
	}

	public AbstractPrimitiveJavaObjectInspector getValueInspector() {
	    return value_inspector.getAnInspector();
	}
    }

    FirstNSelector firstNSelector;

    @Override
    public void close() throws HiveException {
    }
  
    @Override
    public StructObjectInspector initialize(ObjectInspector [] args) throws UDFArgumentException {
	if(args.length != 3 || !KISSInspector.isPrimitive(args[0]) || !KISSInspector.isPrimitive(args[1]) || !KISSInspector.isPrimitive(args[2]))
	    throw new UDFArgumentException("first_n() takes three primitive arguments");

	firstNSelector = new FirstNSelector(args[0], args[1], args[2]);
    
	ArrayList<String> fieldNames = new ArrayList<String>();
	ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	fieldNames.add("group");
	fieldNames.add("value");
	fieldOIs.add(firstNSelector.getGroupInspector());
	fieldOIs.add(firstNSelector.getValueInspector());
	return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object [] o) throws HiveException {
	Object result[] = firstNSelector.getFirstN(o[0], o[1], o[2]);
	if(result != null) 
	    forward(result);
    }
}
