package com.livingsocial.hive.udf;

import com.livingsocial.hive.utils.KISSInspector;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Description(name = "in_array", value = "_FUNC_(x, a) - Returns true if x is in the array a")
public class InArray extends GenericUDF {
    private ListObjectInspector array_inspector;
    private KISSInspector element_inspector;

    static final Log LOG = LogFactory.getLog(InArray.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
	if(args.length != 2 || !KISSInspector.isPrimitive(args[0]) || !KISSInspector.isList(args[1]))
	    throw new UDFArgumentException("in_array() takes two arguments: a primitive and an array");
	element_inspector = new KISSInspector(args[0]);
	array_inspector = (ListObjectInspector) args[1];
	return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    @Override
    public BooleanWritable evaluate(DeferredObject[] arguments) throws HiveException {
	BooleanWritable found = new BooleanWritable(false);
	// if either argument is null, or if the list doesn't contain the same primitives
        if(arguments[0].get() == null || arguments[1].get() == null || !element_inspector.sameAsTypeIn(array_inspector))
	    return found;
	Object element = arguments[0].get();
	Object elist = arguments[1].get();
	for(int i=0; i<array_inspector.getListLength(elist); i++) {
	    LOG.warn("elem from array: " + element_inspector.get(array_inspector.getListElement(elist, i)));
	    LOG.warn("elem: " + element_inspector.get(element));
            Object listElem = array_inspector.getListElement(elist, i);
	    if(listElem != null && element_inspector.get(listElem).equals(element_inspector.get(element)))
		found.set(true);
	}
	return found;
    }

    @Override
    public String getDisplayString(String[] children) {
	assert (children.length == 2);
	return "in_array(" + children[0] + ", " + children[1] + ")";
    }
}
