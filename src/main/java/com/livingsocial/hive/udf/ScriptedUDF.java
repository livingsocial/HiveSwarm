package com.livingsocial.hive.udf;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "scriptedUDF", value = "_FUNC_(script_to_run, language, script_arg1, script_arg_2, ....) " +
		"- Returns a string,string map from the script.", 
		extended = "Example:\n" + " > SELECT _FUNC_(2, 5, 12, 3) FROM src;\n 2")
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
			ObjectInspectorConverters.Converter conv = getConverter(oi);
			if (conv != null) {
				converters[i] = conv;
			} else {
				throw new UDFArgumentLengthException("Could not figure out how to convert argument " + i+2 + " to a UDF type");
			}
		}
		
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		returnOIResolver.update(outputOi);
		return returnOIResolver.get();
	}
	

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if (engine == null) initializeEngine(arguments[0], arguments[1]);
		
		Object[] args = new Object[converters.length];
		for (int i = 0; i < converters.length; i++) {
			args[i] = converters[i].convert(arguments[i+2].get());
		}
		
		Object out;
		try {
			out = engine.invokeFunction("eval", args);
		} catch (Exception e) {
			throw new HiveException("Error invoking the eval function", e);
		}
		return returnOIResolver.convertIfNecessary(out, outputOi);
	}

	private void initializeEngine(DeferredObject scriptObj,
			DeferredObject languageObj) throws HiveException {
		
		String script = scriptConverter.convert(scriptObj.get()).toString();
		String language = languageConverter.convert(languageObj.get()).toString();
		
		ScriptEngine tmp = new ScriptEngineManager().getEngineByName(language);
		if (tmp == null) {
			throw new HiveException("Could not find a script implementation for language " + language);
		}
		
		if (! (tmp instanceof Invocable) ) {
			throw new HiveException("The script engine for " + language + " doesn't support invocable");
		}
		
		engine = (Invocable) tmp;
		
		String scriptText;
		if (script.startsWith("hdfs:")) {
			throw new IllegalArgumentException("Sorry, this isn't supported yet");
		} else {
			// The script is a literal script and should be handled directly
			scriptText = script;
		}
		
		try {
			tmp.eval(scriptText);
		} catch (ScriptException e) {
			throw new IllegalArgumentException("Something went wrong with the script", e);
		}
		
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

	
	private Converter getConverter(ObjectInspector oi) {
		if (Category.PRIMITIVE.equals(oi.getCategory())) {
			PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) oi)
          .getPrimitiveCategory();
			
			if (PrimitiveCategory.STRING.equals(primitiveCategory)) {
				return ObjectInspectorConverters.getConverter(oi,
	          PrimitiveObjectInspectorFactory.javaStringObjectInspector);

			} else if (PrimitiveCategory.DOUBLE.equals(primitiveCategory) ||
					PrimitiveCategory.FLOAT.equals(primitiveCategory)) {
				return ObjectInspectorConverters.getConverter(oi,
	          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
				
			} else if (PrimitiveCategory.INT.equals(primitiveCategory) ||
					PrimitiveCategory.SHORT.equals(primitiveCategory) ||
					PrimitiveCategory.BYTE.equals(primitiveCategory) ||
					PrimitiveCategory.LONG.equals(primitiveCategory)) {
				return ObjectInspectorConverters.getConverter(oi,
	          PrimitiveObjectInspectorFactory.javaLongObjectInspector);
				
			}
			
		}
		
		return null;

	}

}
