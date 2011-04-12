package com.livingsocial.hive.udtf;

import com.livingsocial.hive.utils.KISSInspector;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.description;
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

@description(
    name = "firstN",
    value = "_FUNC_(group_by, values, how_many) - return table of first how_many values by group_by"  //should take an array?
)

public class FirstN extends GenericUDTF {
    private class FirstNSelector {                                 //these are signatures?
	private KISSInspector group_inspector, value_inspector;
	private Object current_group = null;
	private float last_value;                                  //necessary?

	public FirstNSelector(ObjectInspector gpoi, ObjectInspector vpoi) {  //what exact functionality is exposed here?
	    group_inspector = new KISSInspector(gpoi);
	    value_inspector = new KISSInspector(vpoi);
	}
	
	public Object[] getFirstN(Object group, Object value) {              //the meat
	    float new_value = value_inspector.toFloat(value);
	    Object[] result = null;

	    if(!group_inspector.get(group).equals(current_group)) {
		current_group = group_inspector.get(group);
	    } else {
		Float diff = new Float(new_value - last_value);
		result = new Object[] { group_inspector.get(group), diff };
	    }
	    
	    last_value = new_value;
	    return result;
	}

	public AbstractPrimitiveJavaObjectInspector getGroupInspector() {        //To look at arg list, I suppose?
	    return group_inspector.getAnInspector();
	}
    }

    FirstNSelector firstNSelector;

    @Override
    public void close() throws HiveException {
    }
  
    @Override
    public StructObjectInspector initialize(ObjectInspector [] args) throws UDFArgumentException {         //Invoked on creation?
	if(args.length != 3 || !KISSInspector.isPrimitive(args[0]) || !KISSInspector.isPrimitive(args[1]) || !KISSInspector.isPrimitive(args[2]))
	    throw new UDFArgumentException("firstN() takes three primitive arguments");

	firstNSelector = new FirstNSelector(args[0], args[1], args[2]);
    
	ArrayList<String> fieldNames = new ArrayList<String>();
	ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	fieldNames.add("group");
	fieldNames.add("interval");
	fieldOIs.add(firstNSelector.getGroupInspector());
	fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.FLOAT));
	return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object [] o) throws HiveException {                                  //Runs on every line
	Object result[] = firstNSelector.getFirstN(o[0], o[1]);
	if(result != null) 
	    forward(result);
    }
}
