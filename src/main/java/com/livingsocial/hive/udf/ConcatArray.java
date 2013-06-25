package com.livingsocial.hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.livingsocial.hive.utils.KISSInspector;

@Description(name = "concat_array", value = "_FUNC_(sep, arr) - Returns a string with each element in the array arr with sep as the delimiter between elements")
public class ConcatArray extends GenericUDF {
        public static final String FUNC = "concat_array";
    private ListObjectInspector array_inspector;
    private KISSInspector element_inspector;

    static final Log LOG = LogFactory.getLog(InArray.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if(args.length != 2 || !KISSInspector.isPrimitive(args[0]) || !KISSInspector.isList(args[1]))
            throw new UDFArgumentException(FUNC + "() takes two arguments: a primitive and an array");
        element_inspector = new KISSInspector(args[0]);
        array_inspector = (ListObjectInspector) args[1];
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public String evaluate(DeferredObject[] arguments) throws HiveException {
        StringBuilder out = new StringBuilder();
        
        if(arguments[0].get() == null || arguments[1].get() == null)
            return null;
        
        Object element = arguments[0].get();
        String sep = element_inspector.get(element).toString();
        
        Object elist = arguments[1].get();
        boolean first = true;
        int len = array_inspector.getListLength(elist);
        for(int i=0; i<len; i++) {
        	if (first) first = false;
        	else out.append(sep);
        	
            Object listElem = array_inspector.getListElement(elist, i);
            if(listElem != null)
            	out.append(listElem.toString());
        }
        return out.toString();
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 2);
        return FUNC + "(" + children[0] + ", " + children[1] + ")";
    }
}
