package com.livingsocial.hive.udf;

import java.util.HashMap;
import java.util.Map;

import javax.script.Invocable;

import org.apache.hadoop.hive.ql.exec.Description;
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


@Description(name = "scriptedUDAF", value = "_FUNC_(script_to_run, language, script_arg1, script_arg_2, ....) " +
    "- Returns a string,string map from the various functions in the script.  3 functions are needed:  iterate(temp_map, arg1, arg2, ...), merge(temp_map_1, temp_map_2), and terminate(temp_map).", 
    extended = "Example:\n" + 
               " > -- compute the differences between a series of time events in a group \n" +
               " > SELECT group_id, _FUNC_(' \n" +
               "# Accumulate a list of times \n"+
               "def iterate(result, time)\n  {\"rtn\" => (result.fetch(\"rtn\",[]).split(\",\") << time) }  \nend\n" +
               "# Merge the time lists \n"+
               "def merge(result1, result2) \n result1[\"rtn\"] = result1[\"rtn\"].split(\",\").concat(result2[\"rtn\"].split(\",\")) \nend\n" +
               "# Sort the final list and compute diffs \n"+
               "def terminate(result) \n  last=0\n  {\"rtn\" => (result.fetch(\"rtn\",[]).split(\",\").sort().collect(|x| r=x-last; last=x; r) }  \nend\n" +
               "', 'ruby', val1, val2) FROM src_table GROUP BY group_id; \n" +
               "  \n" +
               "\nAlternate syntax:\n> SELECT group_id, _FUNC_('/my_scripts/reusable.rb', 'ruby', val1, val2) FROM src_table GROUP BY group_id; \n" +
               " this will load the script from the location in HDFS. ")
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
