package com.livingsocial.hive.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.livingsocial.hive.utils.ScriptingHelper;
import com.livingsocial.hive.utils.ScriptingHelper.InitializationContainer;

/**
 * This does not work, but is close.  See the FIXME comment in the init function
 */
@UDFType(deterministic = false, stateful = true)
@Description(name = "scriptedUDAF", value = "_FUNC_(script_to_run, language, return_type, script_arg1, script_arg_2, ....) " +
    "- Runs custom UDAF code from the various functions in the script.  The required functions are:  iterate(agg_data, arg1, arg2, ...), partial(agg_data), convert_to_string(agg_data), convert_from_string(agg_data), merge(agg_data1, agg_data2), and terminate(agg_data).", 
    extended = "Function descriptions in the script:\n" +
               "    iterate receives an agg_data object and all the extra arguments in the UDAF call.  On first call agg_data will be null.  The script needs to build an appropriate object, accumulate data from the arguments, and return the agg object.  That agg object will be passed to later calls.  \n" +
               "    partial receives the agg_data object from the iterate call and returns a partial aggregation (this can simply return agg_data)\n" +
               "    convert_to_string receives the partial_results object from the partial call and returns a string\n" +
               "    convert_from_string receives the string from convert_to_string and rebuilds the partial_results object\n" +
               "    merge receives 2 partial_results objects and returns a merged version with data from both\n" +
               "    terminate receives a merged partial_results object and returns the final return object type.  The object returned needs to adhere to the return_type specified in the _FUNC_ call\n" +
               "\nLanguage is the javax.script engine name.  Additional languages can be added by adding the jar implementing the scripting engine ('add jar groovy-all.jar;' or similar)\n" +
               "Return_type is a hive style data definition ('string', 'bigint', 'array<map<string,string>>', ...) \n\n" +
               "Example:\n" + 
               " > -- compute the differences between a series of time events in a group \n" +
               "select person_id, _FUNC_('\n" +
               "    require \"json\\n" +
               " \n" +
               "    def iterate(result, time)\n" +
               "      result ||= []\n" +
               "      result << time.to_i\n" +
               "      result\n" +
               "    end\n" +
               " \n" +
               "    def convert_to_string(result)\n" +
               "      result.to_json\n" +
               "    end\n" +
               " \n" +
               "    def convert_from_string(json)\n" +
               "      JSON.parse(json)\n" +
               "    end\n" +
               " \n" +
               "    def merge(result1, result2)\n" +
               "      result1 ||= []\n" +
               "      result1.concat(result2)\n" +
               "      result1\n" +
               "    end\n" +
               " \n" +
               "    # Note: since this does no partial aggregation it is a bad example of a UDAF\n" +
               "    def partial(times)\n" +
               "      times\n" +
               "    end\n" +
               " \n" +
               "    def terminate(times)\n" +
               "      times.sort!\n" +
               "      last=0\n" +
               "      output = []\n" +
               "      times.each { |time|\n" +
               "        output << t - last\n" +
               "        last = t\n" +
               "      }\n" +
               "      output\n" +
               "    end\n" +
               "                 \n" +
               "    ', 'ruby', 'array<bigint>', 'array<bigint>', purchase_time ) as time_diffs\n" +
               "    from purchases\n" +
               "    group by person_id\n" +
               "  \n" +
               "\nAlternate syntax:\n> SELECT group_id, _FUNC_('/my_scripts/reusable.rb', 'ruby', val1, val2) FROM src_table GROUP BY group_id; \n" +
               " this will load the script from the location in HDFS. ")
public class ScriptedUDAF extends AbstractGenericUDAFResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ScriptedUDAF.class);

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
      throws SemanticException {
    return new ScriptedUDAFEvaluator();
  }

  private static class MyAggBuffer implements AggregationBuffer {
    public Object data;
  }

  public static class ScriptedUDAFEvaluator extends GenericUDAFEvaluator {
    private InitializationContainer initData;

    GenericUDFUtils.ReturnObjectInspectorResolver outputResolver;
    
    // Store all intermediate data in a string/string map
    ObjectInspector intermediateInternal = ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    ObjectInspector intermediateExternal = LazyBinaryObjectInspectorFactory.getLazyBinaryMapObjectInspector(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    
    private ObjectInspectorConverters.Converter intermediateConverterInput = ObjectInspectorConverters.getConverter(intermediateExternal, intermediateInternal);

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      LOG.info("Mode: " + m.name(), new Exception());
      
      
      
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        // This is getting the full arg list
        if (initData == null) initData = ScriptingHelper.initialize(parameters);
        
      } else {
        // This is getting only intermediate data so there's nothing to do here
        if (initData == null) {
          initData = new ScriptingHelper.InitializationContainer();
        }
      }
      
      // Fall back error check
      if (initData == null) {
        throw new HiveException("Something went wrong while trying to initialize things");
      }

      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        outputResolver = ScriptingHelper.buildReturnResolver(intermediateExternal);
      } else if (m == Mode.COMPLETE) {
        outputResolver = ScriptingHelper.buildReturnResolver(initData.outputOi);
      } else {
        // FIXME: 
        // This does not work because there is no way to know what the return type should be
        // If there is a way to pass the constant OI type from the original input to the 
        // mode=FINAL call then this can work.  
        throw new IllegalStateException("This does not work, see the comment in the code");
      }
      return outputResolver.get();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new MyAggBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((MyAggBuffer)agg).data = null;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] arguments)
        throws HiveException {
      LOG.info("Start iterate");
      MyAggBuffer data = (MyAggBuffer) agg;

      Object[] args = new Object[1+arguments.length - initData.argOffset];
      for (int i = 0; i < args.length-1; i++) {
        args[i+1] = ObjectInspectorUtils.copyToStandardJavaObject(arguments[i+initData.argOffset], initData.argumentOIs[i]);
      }
      args[0] = data.data;

      try {
        data.data = initData.engine.invokeFunction("iterate", args);
      } catch (Exception e) {
        throw new HiveException("Error invoking the iterate function", e);
      }
      LOG.info("End iterate");
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      LOG.info("Start terminatePartial");
      MyAggBuffer myAgg = (MyAggBuffer) agg;

      String rtn = null;
      try {
        rtn = initData.engine.invokeFunction("convert_to_string", (Object)myAgg.data).toString();
      } catch (Exception e) {
        throw new HiveException("Error invoking the partial function", e);
      }
      
      Map<String,String> out = new HashMap<String,String>();
      out.put("data", rtn);
      out.put("script", initData.script);
      out.put("language", initData.language);
      out.put("returntype", initData.returnType);
      
      try {
        Object tmp = outputResolver.convertIfNecessary(out, intermediateInternal);
        LOG.info("End terminatePartial");
        return tmp;
      } catch (Exception e) {
        e.printStackTrace(System.err);
        throw new HiveException("bad stuff", e);
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      
      LOG.info("Merging a " + partial.getClass().getName());
      MyAggBuffer myAgg = (MyAggBuffer) agg;
      
      @SuppressWarnings("unchecked")
      Map<String,String> converted = (Map<String,String>) intermediateConverterInput.convert(partial);

      try {
        String script = (String) converted.get("script");
        String language = (String) converted.get("language");
        if (initData.engine == null) {
        initData.engine = ScriptingHelper.initializeEngine(language, script);
      }
        String data = (String) converted.get("data");
        Object convertedData = initData.engine.invokeFunction("convert_from_string", data);

      try {
        myAgg.data = initData.engine.invokeFunction("merge", myAgg.data, convertedData);
      } catch (Exception e) {
        throw new HiveException("Error invoking the merge function", e);
      }
      } catch (Exception e) {
        e.printStackTrace();
        throw new HiveException("bad stuff");
      }
      LOG.info("End merge");
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      LOG.info("Start terminate");
      MyAggBuffer myAgg = (MyAggBuffer) agg;

      try {
        Object out = initData.engine.invokeFunction("terminate", myAgg.data);
        
        LOG.info("End terminate");
        return outputResolver.convertIfNecessary(out, outputResolver.get());
      } catch (Exception e) {
        throw new HiveException("Error invoking the terminate function", e);
      }
      
    }
  }
}
