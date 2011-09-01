package com.livingsocial.hive.udtf;

import java.util.ArrayList;
import java.util.List;

import com.livingsocial.hive.utils.KISSInspector;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


@Description(name = "bin_case",
    value = "_FUNC_(column, (array of bit field names)) - table generating function that spits out column name for each bit position that is a 1")
public class BinCase extends GenericUDTF {

    private class BinCaseSelector {
	private KISSInspector bitfield_inspector;
	private ListObjectInspector bitnames_inspector;
	private UDFBin b;
	private ArrayList result;
	public BinCaseSelector(ObjectInspector bfi, ObjectInspector bni) {
	    bitfield_inspector = new KISSInspector(bfi);
	    bitnames_inspector = (ListObjectInspector) bni;
	    result = new ArrayList();
	    b = new UDFBin();
	}

	public ArrayList getNames(Object bf, Object bn) {
	    result.clear();
	    List<?> names = bitnames_inspector.getList(bn);
	    long value = bitfield_inspector.toLong(bf);
	    String values = b.evaluate(new LongWritable(value)).toString();
	    for(int i=(values.length()-1); i >= 0; i--) {
		if(values.charAt(i) == '1' && i < names.size()) {
		    result.add(names.get(i));
		}
	    }
	    return result;
	}	

	public ObjectInspector getNamesInspector() {
	    return bitnames_inspector.getListElementObjectInspector();
        }
    }


    private BinCaseSelector bcs = null;

    @Override
    public void close() throws HiveException {
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
	if(args.length != 2 || !KISSInspector.isPrimitive(args[0]) || !KISSInspector.isList(args[1]))
	    throw new UDFArgumentException("bin_case() takes two arguments: an integer and an array of strings");

	bcs = new BinCaseSelector(args[0], args[1]);

	ArrayList<String> fieldNames = new ArrayList<String>();
	ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	fieldNames.add("col");
	fieldOIs.add(bcs.getNamesInspector());
	return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    
    private final Object[] forwardObj = new Object[1];

    @Override
    public void process(Object[] o) throws HiveException {
	ArrayList names = bcs.getNames(o[0], o[1]);
	for (Object r : names) {
	    forwardObj[0] = r;
	    forward(forwardObj);
	}
    }

    @Override
    public String toString() {
	return "bin_case";
    }
}
