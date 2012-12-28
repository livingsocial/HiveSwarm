package com.livingsocial.hive.udf;

import com.livingsocial.hive.utils.KISSInspector;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Description(name = "index_of_max_elem", value = "_FUNC_(a) - Returns index of maximum element of the array a")
public class IndexOfMaxElem extends GenericUDF {
    private ListObjectInspector array_inspector;
    private KISSInspector elem_inspector;
    static final Log LOG = LogFactory.getLog(InArray.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
	if(args.length != 1 || !KISSInspector.isList(args[0]))
	    throw new UDFArgumentException("index_of_max_elem() takes one argument: an array");
	array_inspector = (ListObjectInspector) args[0];
	elem_inspector = new KISSInspector(array_inspector.getListElementObjectInspector());
	return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public IntWritable evaluate(DeferredObject[] arguments) throws HiveException {
	Object elist = arguments[0].get();
	
	//Possibly check for Comparable here
	if(elist == null || array_inspector.getListLength(elist) == 0)
	    return new IntWritable(-1);
	
	IntWritable maxIndex = new IntWritable(0);
	Comparable maxVal = (Comparable) elem_inspector.get(array_inspector.getListElement(elist,0));

	for(int i=0; i<array_inspector.getListLength(elist); i++) {
	    LOG.warn("elem from array: " + array_inspector.getListElement(elist, i));
	    Comparable listElem = (Comparable)elem_inspector.get(array_inspector.getListElement(elist, i));
	    if(listElem != null && listElem.compareTo(maxVal) > 0) {
		maxVal = listElem;
		maxIndex = new IntWritable(i);
	    }
	}
	return maxIndex;
    }

    @Override
    public String getDisplayString(String[] children) {
	assert (children.length == 1);
	return "index_of_max_elem(" + children[0] + ")";
    }
}
