package com.livingsocial.hive.udf;

import javax.script.Invocable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.livingsocial.hive.utils.ScriptingHelper;

@Description(name = "scriptedUDF", value = "_FUNC_(script_to_run, language, script_arg1, script_arg_2, ....) " +
    "- Returns a string,string map from evaluate function of the script.", 
    extended = "Example:\n" + " > SELECT _FUNC_('def evaluate(val1, val2) \n  {\"rtn\" => \"#{val1} concat #{val2}\" }  \nend', 'ruby', val1, val2) FROM src_table; \n" +
               " the script needs a function named 'evaluate' that will be invoked with the passed in args.  This function needs to return a map of string/string pairs. \n" +
               "\nAlternate syntax:\n> SELECT _FUNC_('/my_scripts/reusable.rb', 'ruby', val1, val2) FROM src_table; \n" +
               " this will load the script from the location in HDFS. ")
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
    if (engine == null) engine = ScriptingHelper.initializeEngine(arguments[0].get(), arguments[1].get(), initData.scriptConverter, initData.languageConverter);
    
    Object[] args = new Object[initData.converters.length];
    for (int i = 0; i < initData.converters.length; i++) {
      args[i] = initData.converters[i].convert(arguments[i+2].get());
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
