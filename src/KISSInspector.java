package com.livingsocial.hive.udtf;

import java.util.ArrayList;
import java.util.List;

import com.livingsocial.hive.Utils;

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

public class KISSInspector {
    private PrimitiveObjectInspector inspector;
    
    public KISSInspector(ObjectInspector arg) {
	inspector = (PrimitiveObjectInspector) arg;
    }

    public PrimitiveCategory getCategory() {
	return inspector.getPrimitiveCategory();
    }

    public AbstractPrimitiveJavaObjectInspector getAnInspector() {
	return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(getCategory());
    }

    public Object get(Object value) {
	return inspector.getPrimitiveJavaObject(value);
    }

    public float toFloat(Object value) {
	float result;
	value = get(value);
	switch(getCategory()) {
	case STRING:
	    long timestamp = Utils.stringToTimestamp((String) value);
	    result = (timestamp < 0) ? 0L : (new Long(timestamp)).floatValue();
	    break;
	case BOOLEAN:
	    result = ((Boolean) value).booleanValue() ? 1.0f : 0.0f;
	    break;
	case UNKNOWN:
	    result = 0.0f;
	    break;
	case VOID:
	    result = 0.0f;
	    break;
	default: // all other types are numerical
	    result = ((Number) value).floatValue();
	    break;
	}
	return result;
    }

    public boolean equal(Object first, Object second) {
	if((first == null && second != null) || (first != null && second == null))
	    return false;
	if(first == null && second == null)
	    return true;

	first = get(first);
	second = get(second);
	boolean result;
	switch(getCategory()) {
	case STRING:
	    result = ((String) first).compareTo((String) second) == 0;
	    break;
	case BOOLEAN:
	    result = ((Boolean) first).equals((Boolean) second);
	    break;
	case UNKNOWN:
	    result = first.equals(second);
	    break;
	case VOID:
	    result = first.equals(second);
	    break;
	default: // all other types are numerical
	    result = first.equals(second);
	    break;
	}
	return result;
    }
}
