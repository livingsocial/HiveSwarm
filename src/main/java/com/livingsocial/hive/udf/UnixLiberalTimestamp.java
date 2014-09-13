package com.livingsocial.hive.udf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnixTimeStamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;

@Description(
	     name = "unix_liberal_timestamp",
	     value = "_FUNC_(str) - gets unix timestamp in either yyyy-MM-dd HH:mm:ss or yyyy-MM-dd format - returns null if input is null",
	     extended = "Example:\n" +
	     "  > SELECT a.* FROM srcpart a WHERE _FUNC_ (a.hr) < unix_timestamp() LIMIT 1;\n"
	     )
public class UnixLiberalTimestamp extends GenericUDFUnixTimeStamp {

    private transient StringObjectInspector stringOI;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(getName().toUpperCase() + " only takes 1 arguments: String");
        }
        super.initialize(new ObjectInspector[]{arguments[0], arguments[0]});
        stringOI = (StringObjectInspector) arguments[0];
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String datestring = stringOI.getPrimitiveJavaObject(arguments[0].get());
        if (datestring == null) return null;
        if (datestring.length() != 10) // timestamp
            return super.evaluate(new DeferredObject[]{arguments[0],
                                                       new DeferredJavaObject(new Text("yyyy-MM-dd HH:mm:ss"))});
        else                           // date
            return super.evaluate(new DeferredObject[]{arguments[0],
                                                       new DeferredJavaObject(new Text("yyyy-MM-dd"))});
    }
}
