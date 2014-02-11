package com.livingsocial.hive.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class ScriptingHelper {
	
	// Offset to get past the constants in the arg list
	private static int ARG_OFFSET = 3;
  
  public static class InitializationContainer {
    public String script;
    public String language;
    public ObjectInspector[] argumentOIs;

    public String returnType;
    public ObjectInspector outputOi;
    public ObjectInspector outputJavaOi;
    public GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    
    public int argOffset = ARG_OFFSET;
    public Invocable engine;
    
  }
  
  public static InitializationContainer initialize(ObjectInspector[] arguments)
      throws SemanticException {

    InitializationContainer rtn = new InitializationContainer();
    
    if (arguments != null) {
      // Only validate inputs if this is the main call, for merging and terminating this step is not needed.

      // Nothing else can really be validated until evaluation time
      if (arguments.length < rtn.argOffset+1) {
        throw new SemanticException(
            "At least " + (rtn.argOffset+1) + " arguments are required, the script to run, the script language, the return type, and at least one argument, got "
                + arguments.length + " arguments passed in");
      }

      // Convert all the constant string params
      rtn.script = getConstString(arguments[0], 1);
      rtn.language = getConstString(arguments[1], 2);
      rtn.returnType = getConstString(arguments[2], 3);
      
      // Get converters for all the actual arguments
      rtn.argumentOIs = new ObjectInspector[arguments.length - rtn.argOffset];
      System.arraycopy(arguments, rtn.argOffset, rtn.argumentOIs, 0, rtn.argumentOIs.length);

      rtn.outputOi = javaObjectInspectorFromType(rtn.returnType);
      rtn.outputJavaOi = javaObjectInspectorFromType(rtn.returnType);
      rtn.returnOIResolver = buildReturnResolver(rtn.outputOi);
      
    }
    
    // Do this to test the script and make sure it's valid client-side
    try {
			rtn.engine = initializeEngine(rtn.language, rtn.script);
		} catch (HiveException e) {
			throw new SemanticException("The Script or Language settings seem to have problems: " + e, e);
		}
    
    return rtn;
  }

	public static String getConstString(ObjectInspector argument, int num)
			throws UDFArgumentTypeException {
		if (!ObjectInspectorUtils.isConstantObjectInspector(argument)) {
			new Exception().printStackTrace(System.err);
		  throw new UDFArgumentTypeException(num,
		      "The script argument " + num + " must be a constant string, but "
		          + argument.getTypeName() + " was passed instead. (class=" + argument.getClass().getName() + ")");
		}
		return (String) ObjectInspectorConverters.getConverter(
        argument,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector).convert(((ConstantObjectInspector)argument).getWritableConstantValue());
	}
  
  public static ObjectInspector javaObjectInspectorFromType(String returnType) throws SemanticException {
  	return TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoUtils.getTypeInfoFromTypeString(returnType));
  }
  
  public static ObjectInspector writableObjectInspectorFromType(String returnType) throws SemanticException {
  	return TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoUtils.getTypeInfoFromTypeString(returnType));
  }
  
  public static GenericUDFUtils.ReturnObjectInspectorResolver buildReturnResolver(ObjectInspector outputOi) throws SemanticException {
    GenericUDFUtils.ReturnObjectInspectorResolver rtn = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    rtn.update(outputOi);
    return rtn;
  }
  
  public static Converter getConverter(ObjectInspector oi) {
    ObjectInspector output = ObjectInspectorUtils.getStandardObjectInspector(oi);
    return output != null ? ObjectInspectorConverters.getConverter(oi, output) : null;
  }

  /**
   * Builds an invocable scripting engine using the passed in args.
   * This loads and compiles the script so the functions are available
   * in the returned Invocable engine.
   */
  public static Invocable initializeEngine(String language, String script) throws HiveException {
    
    // Make sure we can find a scripting engine for the language
    ScriptEngine tmp = new ScriptEngineManager().getEngineByName(language);
    if (tmp == null) {
      throw new HiveException(
          "Could not find a script implementation for language " + language);
    }

    if (!(tmp instanceof Invocable)) {
      throw new HiveException("The script engine for " + language
          + " doesn't support invocable");
    }

    Invocable engine = (Invocable) tmp;

    String scriptText;
    if (script.startsWith("/")) {
      // The file is a file in HDFS

      // Note: this is not the best way to do this, but it works
      Configuration conf = new Configuration();
      String root = conf.get("fs.defaultFS");
      String path = root + script;
      try {
        FileSystem fs = FileSystem.get(conf);
        Path scriptFile = new Path(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(
            fs.open(scriptFile)));
        String line = null;
        StringBuilder scriptBuilder = new StringBuilder();
        while ((line = reader.readLine()) != null) {
          scriptBuilder.append(line);
          scriptBuilder.append("\n");
        }
        scriptText = scriptBuilder.toString();
      } catch (IOException e) {
        throw new HiveException(
            "Unable to load the script from file " + script, e);
      }
    } else {
      // The script is a literal script and should be handled directly
      scriptText = script;
    }

    try {
      tmp.eval(scriptText);
    } catch (ScriptException e) {
      throw new HiveException(
          "Something went wrong with the script when evaluating it", e);
    }

    return engine;

  }

}
