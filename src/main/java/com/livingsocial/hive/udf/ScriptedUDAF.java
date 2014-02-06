package com.livingsocial.hive.udf;

import java.util.HashMap;
import java.util.Map;

import javax.script.Invocable;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.livingsocial.hive.utils.ScriptingHelper;
import com.livingsocial.hive.utils.ScriptingHelper.InitializationContainer;

public class ScriptedUDAF extends AbstractGenericUDAFResolver {

	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
			throws SemanticException {
		return new ScriptedUDAFEvaluator(ScriptingHelper.initialize(info.getParameterObjectInspectors()));
	}

	private static class MyAggBuffer implements AggregationBuffer {
		Map<String, String> buffer = new HashMap<String, String>();
	}

	public static class ScriptedUDAFEvaluator extends GenericUDAFEvaluator {
		static final String LANG = "__LANG";
		static final String SCRIPT = "__SCRIPT";
		private InitializationContainer initData;
		private Invocable engine;
		private ObjectInspectorConverters.Converter intermediateConverterOutput;
		private ObjectInspectorConverters.Converter intermediateConverterInput;
		
		public ScriptedUDAFEvaluator() {}
		
		public ScriptedUDAFEvaluator(InitializationContainer initData) {
			this.initData = initData;
		}
		
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			
			ObjectInspector rtn = null;
			
      if (m == Mode.PARTIAL1 ) {
      	if (initData == null)
      		initData = ScriptingHelper.initialize(parameters);
      	rtn = initData.returnOIResolver.get();
      } else {
    		initData = ScriptingHelper.initialize(null);
      	rtn = ScriptingHelper.buildReturnResolver(ScriptingHelper.buildOutputOi()).get();
      }
      
      
      ObjectInspector intermediateInternal = ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      ObjectInspector intermediateExternal = LazyBinaryObjectInspectorFactory.getLazyBinaryMapObjectInspector(
          PrimitiveObjectInspectorFactory.writableStringObjectInspector, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      
      intermediateConverterInput = ObjectInspectorConverters.getConverter(intermediateExternal, intermediateInternal);
      intermediateConverterOutput = ObjectInspectorConverters.getConverter(intermediateInternal, intermediateExternal);
      
      return rtn;
		}
		
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			return new MyAggBuffer();
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((MyAggBuffer) agg).buffer.clear();
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] arguments)
				throws HiveException {
			if (engine == null) {
				engine = ScriptingHelper.initializeEngine(arguments[0], arguments[1],
						initData.scriptConverter, initData.languageConverter);
			}
			
			MyAggBuffer data = (MyAggBuffer) agg;
			
			// Hack for ensuring the script and language get passed on to later stages
			data.buffer.put(SCRIPT, (String) initData.scriptConverter.convert(arguments[0]));
			data.buffer.put(LANG, (String) initData.languageConverter.convert(arguments[1]));

			Object[] args = new Object[initData.converters.length + 1];
			for (int i = 0; i < initData.converters.length; i++) {
				args[i + 1] = initData.converters[i].convert(arguments[i + 2]);
			}
			args[0] = data.buffer;

			try {
				engine.invokeFunction("iterate", args);
			} catch (Exception e) {
				throw new HiveException("Error invoking the iterate function", e);
			}

			System.out.println("iterate: " + data.buffer.keySet() + "  L:" + data.buffer.get(LANG) + "   S:" + data.buffer.get(SCRIPT));
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			MyAggBuffer myAgg = (MyAggBuffer) agg;
			return intermediateConverterOutput.convert(myAgg.buffer);
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			
			Map converted = (Map) intermediateConverterInput.convert(partial);
			
			MyAggBuffer myAgg = (MyAggBuffer) agg;
			
			initEngine(converted);
			
			// Re-copy the lang and script
			myAgg.buffer.put(SCRIPT, (String) converted.get(SCRIPT));
			myAgg.buffer.put(LANG, (String) converted.get(LANG));

			try {
				engine.invokeFunction("merge", myAgg.buffer, converted);
			} catch (Exception e) {
				throw new HiveException("Error invoking the merge function", e);
			}
		}

		@SuppressWarnings("rawtypes")
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			MyAggBuffer myAgg = (MyAggBuffer) agg;
			initEngine(myAgg.buffer);

			try {
				Map out = (Map) engine.invokeFunction("terminate", myAgg.buffer);
				
				return initData.returnOIResolver.convertIfNecessary(out, initData.outputOi);
			} catch (Exception e) {
				throw new HiveException("Error invoking the merge function", e);
			}
			
		}
		
		private void initEngine(Map<String,String> map) throws HiveException {
			if (engine == null)
				engine = ScriptingHelper.initializeEngine(map.get(LANG), map.get(SCRIPT));
				
		}
	}
}
