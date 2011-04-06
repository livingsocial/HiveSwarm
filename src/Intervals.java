package com.livingsocial.hive.udtf;

import com.livingsocial.hive.udtf.KISSInspector;

import java.util.ArrayList;
import java.util.List;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;

@description(
    name = "intervals",
    value = "_FUNC_(group_by, values) - get all intervals between values by group_by"
)

public class Intervals extends GenericUDTF {
    private class Intervaler {
	private KISSInspector group_inspector, value_inspector;
	private Object current_group = null;
	private float last_value;

	public Intervaler(ObjectInspector gpoi, ObjectInspector vpoi) {
	    group_inspector = new KISSInspector(gpoi);
	    value_inspector = new KISSInspector(vpoi);
	}
	
	public Object[] getInterval(Object group, Object value) {
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

	public AbstractPrimitiveJavaObjectInspector getGroupInspector() {
	    return group_inspector.getAnInspector();
	}
    }

    Intervaler intervaler;

    @Override
    public void close() throws HiveException {
    }
  
    @Override
    public StructObjectInspector initialize(ObjectInspector [] args) throws UDFArgumentException {
	if (args.length != 2) 
	    throw new UDFArgumentException("intervals() takes two arguments");
	
	if(args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) 
	    throw new UDFArgumentException("first argument to intervals must be a primitive");

	if(args[1].getCategory() != ObjectInspector.Category.PRIMITIVE) 
	    throw new UDFArgumentException("second argument to intervals must be a primitive");

	intervaler = new Intervaler(args[0], args[1]);
    
	ArrayList<String> fieldNames = new ArrayList<String>();
	ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	fieldNames.add("group");
	fieldNames.add("interval");
	fieldOIs.add(intervaler.getGroupInspector());
	fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.FLOAT));
	return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object [] o) throws HiveException {
	Object result[] = intervaler.getInterval(o[0], o[1]);
	if(result != null) 
	    forward(result);
    }
}
