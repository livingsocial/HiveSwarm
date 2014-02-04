package com.livingsocial.hive.udf;

import javax.script.Invocable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.livingsocial.hive.utils.ScriptingHelper;

@Description(name = "scriptedUDF", value = "_FUNC_(script_to_run, language, script_arg1, script_arg_2, ....) " +
    "- Returns a string,string map from evaluate function of the script.", 
    extended = "Example:\n" + " > SELECT _FUNC_('def evaluate(val1, val2) \n  {\"rtn\" => \"#{val1} concat #{val2}\" }  \nend', 'ruby', val1, val2) FROM src_table; \n" +
               " the script needs a function named 'evaluate' that will be invoked with the passed in args.  This function needs to return a map of string/string pairs. \n" +
               "\nAlternate syntax:\n> SELECT _FUNC_('/my_scripts/reusable.rb', 'ruby', val1, val2) FROM src_table; \n" +
               " this will load the script from the location in HDFS. ")
public class ScriptedUDF extends GenericUDF {

  private ObjectInspector[] argumentOIs;
  private ObjectInspectorConverters.Converter[] converters;
  
  private Invocable engine;

  private ObjectInspectorConverters.Converter scriptConverter;
  private ObjectInspectorConverters.Converter languageConverter;
  
  private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
  private ObjectInspector outputOi = ObjectInspectorFactory.getStandardMapObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    // Nothing else can really be validated until evaluation time
    if (arguments.length < 3) {
      throw new UDFArgumentLengthException("At least 3 arguments are required, the script to run, the script language, and at least one argument");
    }
    
    scriptConverter = ObjectInspectorConverters.getConverter(arguments[0],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    languageConverter = ObjectInspectorConverters.getConverter(arguments[1],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    
    argumentOIs = new ObjectInspector[arguments.length - 2];
    System.arraycopy(arguments, 2, argumentOIs, 0, argumentOIs.length);
    
    converters = new ObjectInspectorConverters.Converter[argumentOIs.length];
    for (int i = 0; i < argumentOIs.length; i++) {
      ObjectInspector oi = argumentOIs[i];
      ObjectInspectorConverters.Converter conv = ScriptingHelper.getConverter(oi);
      if (conv != null) {
        converters[i] = conv;
      } else {
        throw new UDFArgumentLengthException("Could not figure out how to convert argument " + (i+2) + " to a UDF type");
      }
    }
    
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    returnOIResolver.update(outputOi);
    return returnOIResolver.get();
  }
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (engine == null) engine = ScriptingHelper.initializeEngine(arguments[0], arguments[1], scriptConverter, languageConverter);
    
    Object[] args = new Object[converters.length];
    for (int i = 0; i < converters.length; i++) {
      args[i] = converters[i].convert(arguments[i+2].get());
    }
    
    Object out;
    try {
      out = engine.invokeFunction("evaluate", args);
    } catch (Exception e) {
      throw new HiveException("Error invoking the evaluate function", e);
    }
    return returnOIResolver.convertIfNecessary(out, outputOi);
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("scriptedUDF(");
    for (int i = 0; i < children.length; i++) {
      if ( i != 0 ) sb.append(", ");
      sb.append(children);
    }
    sb.append(")");
    return sb.toString();
  }

}
