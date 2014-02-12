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
    "- Returns the specified return_type (hive style types) from the evaluate function of the script.", 
    extended =  "Function descriptions in the script:\n" +
        "    evaluate receives all the extra script_arguments passed in the _FUNC_ call and returns an object adhering to the defined return_type \n" +
        "\nLanguage is the javax.script engine name.  Additional languages can be added by adding the jar implementing the scripting engine ('add jar groovy-all.jar;' or similar)\n" +
        "Return_type is a hive style data definition ('string', 'bigint', 'array<map<string,string>>', ...) \n\n" +
        "Example:\n > -- Gather complex data combining groups and individual rows without joins \n" +
        "  select person_id, purchase_data['time'], purchase_data['diff'], \n" +
        "    purchase_data['product'], purchase_data['purchase_count'] as pc,\n" +
        "    purchase_data['blah']\n" +
        "  from (\n" +
        "    select person_id, scriptedUDF('\n" +
        "  require \"json\"\n" +
        "  def evaluate(data)\n" +
        "    # This gathers all the data about purchases by person in one place so complex infromation can be gathered while avoiding complex joins \n" +
        "    # Note:  In order for this to work all the data passed into _FUNC_ for a row needs to fit into memory \n" +
        "    tmp = []  # convert things over to a ruby array\n" +
        "    tmp.concat(data)\n" +
        "    tmp.sort_by! { |a| a.get(\"time\") } # for the time differences\n" +
        "    last=0\n" +
        "    tmp.map{ |row| \n" +
        "      # Compute the time difference between purchases and add the total purchase count per person\n" +
        "      t = row[\"time\"] \n" +
        "    \n" +
        "      # The parts that would be much more difficult to generate with SQL \n" +
        "      row[\"diff\"] = t - last\n" +
        "      row[\"purchase_count\"] = tmp.length\n" +
        "      row[\"first_purchase\"] = tmp[0][\"time\"]\n" +
        "      row[\"last_purchase\"] = tmp[-1][\"time\"]\n" +
        "   \n" +
        "      # This shows that built-in libraries are available\n" +
        "      row[\"blah\"] = JSON.generate({\"id\" => row[\"id\"]})\n" +
        "      last = t\n" +
        "      row\n" +
        "    }\n" +
        "  end', 'ruby', 'array<map<string,string>>', \n" +
        "         -- gather all the data about purchases by people so it can all be passed into the evaluate function \n" +
        "         bh_collect(map(   -- Note, bh_collect is from Klouts Brickhouse and allows collecting any type, see https://github.com/klout/brickhouse/ \n" +
        "            'time', unix_liberal_timestamp(purchase_time), \n" +
        "            'product', product_id)) ) as all_data \n" +
        "      from purchases\n" +
        "     group by person_id\n" +
        "  ) foo \n" +
        "  -- explode the data back out so it is available in flattened form \n" +
        "  lateral view explode(all_data) bar as purchase_data \n" +
        "\n" +
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
