package com.livingsocial.hive.udf;

import javax.script.Invocable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import com.livingsocial.hive.utils.ScriptingHelper;

@UDFType(deterministic = false, stateful = true)
@Description(name = "scriptedUDF", value = "_FUNC_(script_to_run, language, return_type, script_arg1, script_arg_2, ....) " +
    "- Returns the specified return_type (hive style types) from evaluate function of the script.", 
    extended = "Example:\n" + " > SELECT _FUNC_('def evaluate(val1, val2) \n  \"#{val1} concat #{val2}\"  \nend', 'ruby', 'string', val1, val2) FROM src_table; \n" +
               " the script needs a function named 'evaluate' that will be invoked with the passed in args. \n" +
               "\nAlternate syntax:\n> SELECT _FUNC_('/my_scripts/reusable.rb', 'ruby', 'map<string,int>', val1, val2) FROM src_table; \n" +
               " this will load the script from the location in HDFS and will invoke the evaluate function.  This function needs to return a map of strings keys and int values. ")
public class ScriptedUDF extends GenericUDF {

  private ScriptingHelper.InitializationContainer initData;
  
  private Invocable engine;
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    
    try {
      initData = ScriptingHelper.initialize(arguments);
    } catch (SemanticException e) {
      throw new UDFArgumentException(e);
    }
    
    return initData.returnOIResolver.get();
  }
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (engine == null) engine = ScriptingHelper.initializeEngine(initData.language, initData.script);
    
    Object[] args = new Object[arguments.length - initData.argOffset];
    for (int i = 0; i < args.length; i++) {
      args[i] = ObjectInspectorUtils.copyToStandardJavaObject(arguments[i+initData.argOffset].get(), initData.argumentOIs[i]);
    }
    
    Object out;
    try {
      out = engine.invokeFunction("evaluate", args);
    } catch (Exception e) {
      throw new HiveException("Error invoking the evaluate function", e);
    }
    return initData.returnOIResolver.convertIfNecessary(out, initData.outputOi);
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
