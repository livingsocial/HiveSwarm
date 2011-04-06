package com.livingsocial.hive.udf;

import com.livingsocial.hive.Utils;
import com.livingsocial.hive.utils.KISSInspector;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

@description(
    name = "min_date",
    value = "_FUNC_(a1, a2, ...) - Returns the smallest non-null date argument",
    extended = "Example:\n" +
        "  > SELECT _FUNC_('2011-01-01 10:11:00', NULL, '2011-02-01', NULL) FROM src LIMIT 1;\n" +
        "  '2011-01-01 10:11:00"
    )
public final class MinDate extends GenericUDF {
    KISSInspector[] inspectors;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
	throws UDFArgumentTypeException {

	inspectors = new KISSInspector[arguments.length];

	for(int i=0; i<arguments.length; i++) {
	    if(!KISSInspector.isPrimitive(arguments[i]))
		throw new UDFArgumentTypeException(i, "min_date takes only date or datetime string arguments");

	    inspectors[i] = new KISSInspector(arguments[i]);
	    if(!inspectors[i].isNull() && !inspectors[i].isString())
		throw new UDFArgumentTypeException(i, "min_date takes only date or datetime string arguments");
	}

	return arguments[0];
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
	Object best = null;
	long best_time = -1L;

	for (int i=0; i<arguments.length; i++) {
	    Object ai = arguments[i].get();
	    if (ai != null) {
		// this will autoconvert string to long timestamp
		long ts = Utils.stringToTimestamp((String) inspectors[i].get(ai));
		if(best_time == -1L || ts < best_time) {
		    best = ai;
		    best_time = ts;
		}
	    }
	}
	return best;
    }

    @Override
    public String getDisplayString(String[] children) {
	StringBuilder sb = new StringBuilder();
	sb.append("min_date(");
	if (children.length > 0) {
	    sb.append(children[0]);
	    for(int i=1; i<children.length; i++) {
		sb.append(",");
		sb.append(children[i]);
	    }
	}
	sb.append(")");
	return sb.toString();
    }
}
