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
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ScriptingHelper {
  
  public static class InitializationContainer {
    public ObjectInspectorConverters.Converter scriptConverter;
    public ObjectInspectorConverters.Converter languageConverter;
    public ObjectInspector[] argumentOIs;
    public ObjectInspectorConverters.Converter[] converters;

    public MapObjectInspector outputOi;
    public GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    
  }
  
  /**
   * 
   */
  public static InitializationContainer initialize(ObjectInspector[] arguments)
      throws SemanticException {

    InitializationContainer rtn = new InitializationContainer();
    
    if (arguments != null) {
      // Only validate inputs if this is the main call, for merging and terminating this step is not needed.

      // Nothing else can really be validated until evaluation time
      if (arguments.length < 3) {
        throw new SemanticException(
            "At least 3 arguments are required, the script to run, the script language, and at least one argument, got "
                + arguments.length + " arguments passed in");
      }

      // Ensure the script and language are constant parameters
      if (!ObjectInspectorUtils.isConstantObjectInspector(arguments[0])) {
        throw new UDFArgumentTypeException(1,
            "The script argument must be a constant, but "
                + arguments[0].getTypeName() + " was passed instead.");
      }

      if (!ObjectInspectorUtils.isConstantObjectInspector(arguments[1])) {
        throw new UDFArgumentTypeException(1,
            "The language argument must be a constant, but "
                + arguments[1].getTypeName() + " was passed instead.");
      }

      // Get type converters for the script and language
      rtn.scriptConverter = ObjectInspectorConverters.getConverter(
          arguments[0],
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      rtn.languageConverter = ObjectInspectorConverters.getConverter(
          arguments[1],
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);

      // Get converters for all the actual arguments
      rtn.argumentOIs = new ObjectInspector[arguments.length - 2];
      System
          .arraycopy(arguments, 2, rtn.argumentOIs, 0, rtn.argumentOIs.length);

      rtn.converters = new ObjectInspectorConverters.Converter[rtn.argumentOIs.length];
      for (int i = 0; i < rtn.argumentOIs.length; i++) {
        ObjectInspector oi = rtn.argumentOIs[i];
        ObjectInspectorConverters.Converter conv = ScriptingHelper
            .getConverter(oi);
        if (conv != null) {
          rtn.converters[i] = conv;
        } else {
          throw new SemanticException(
              "Could not figure out how to convert argument " + (i + 2)
                  + " to a UDF type");
        }
      }
    }
    
    rtn.outputOi = buildOutputOi();
    rtn.returnOIResolver = buildReturnResolver(rtn.outputOi);
    
    return rtn;
  }
  
  public static MapObjectInspector buildOutputOi() {
    return ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
  }
  public static GenericUDFUtils.ReturnObjectInspectorResolver buildReturnResolver(ObjectInspector outputOi) throws SemanticException {
    GenericUDFUtils.ReturnObjectInspectorResolver rtn = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    rtn.update(outputOi);
    return rtn;
  }
  

  /**
   * Get a Converter that will convert the incoming data type into it's standard
   * java type.  This will change numerics to either a double or long.
   * Strings, Lists, and Maps will be standard java types.  
   */
  public static Converter getConverter(ObjectInspector oi) {
    ObjectInspector output = getOutputType(oi);
    return output != null ? ObjectInspectorConverters.getConverter(oi, output) : null;
  }
  
  /**
   * Get the type for things to be converted into based on the incoming type.
   * see the getConverter method for more details. 
   */
  public static ObjectInspector getOutputType(ObjectInspector oi) {
    if (Category.PRIMITIVE.equals(oi.getCategory())) {
      PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) oi)
          .getPrimitiveCategory();

      if (PrimitiveCategory.STRING.equals(primitiveCategory)) {
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;

      } else if (PrimitiveCategory.DOUBLE.equals(primitiveCategory)
          || PrimitiveCategory.FLOAT.equals(primitiveCategory)) {
        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

      } else if (PrimitiveCategory.INT.equals(primitiveCategory)
          || PrimitiveCategory.SHORT.equals(primitiveCategory)
          || PrimitiveCategory.BYTE.equals(primitiveCategory)
          || PrimitiveCategory.LONG.equals(primitiveCategory)) {
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;

      }
      
    } else if (Category.LIST.equals(oi.getCategory())) {
      ListObjectInspector loi = (ListObjectInspector) oi;
      return ObjectInspectorFactory.getStandardListObjectInspector(
          getOutputType(loi.getListElementObjectInspector()));
      
    } else if (Category.MAP.equals(oi.getCategory())) {
      MapObjectInspector moi = (MapObjectInspector) oi;
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          getOutputType(moi.getMapKeyObjectInspector()), getOutputType(moi.getMapValueObjectInspector()));
    }

    return null;
  }

  /**
   * Builds an invocable scripting engine using the passed in args
   */
  public static Invocable initializeEngine(Object scriptObj,
      Object languageObj,
      ObjectInspectorConverters.Converter scriptConverter,
      ObjectInspectorConverters.Converter languageConverter)
      throws HiveException {

    String script = (String) scriptConverter.convert(scriptObj);
    String language = (String) languageConverter.convert(languageObj);
    return initializeEngine(language, script);
  }
  
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
