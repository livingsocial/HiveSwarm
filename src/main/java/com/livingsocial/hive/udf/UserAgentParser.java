package com.livingsocial.hive.udf;

import java.text.ParseException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import ua_parser.Parser;
import ua_parser.Client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@UDFType(deterministic = true)
@Description(name = "user_agent_parser",
         value = "_FUNC_(string, string) - returns parsed information about a user agent string",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3','os_major') FROM src LIMIT 1;\n"
         + "  iOS 5 \n")
public class UserAgentParser extends GenericUDF {

  private Text result = new Text();
  private ObjectInspectorConverters.Converter[] converters;
  static final Log LOG = LogFactory.getLog(UserAgentParser.class.getName());

  private static final Parser uaParser;
  static {
    try {
      uaParser = new Parser();
    }
    catch(IOException e) {
      LOG.warn("Caught IOException: " + e.getMessage());
      throw new RuntimeException("could not instantiate parser");
    }
  }
  

  private enum userOptions {
	os, os_family, os_major, os_minor, ua, ua_family, ua_major, ua_minor, device
  }

  public UserAgentParser() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length > 2 || arguments.length == 0) {
      throw new UDFArgumentLengthException("_FUNC_ expects exactly 2 arguments");
    }
    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].getCategory() != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i,
            "A string argument was expected but an argument of type " + arguments[i].getTypeName()
                + " was given.");

      }

      // Now that we have made sure that the argument is of primitive type, we can get the primitive
      // category
      PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) arguments[i])
          .getPrimitiveCategory();

      if (primitiveCategory != PrimitiveCategory.STRING
          && primitiveCategory != PrimitiveCategory.VOID) {
        throw new UDFArgumentTypeException(i,
            "A string argument was expected but an argument of type " + arguments[i].getTypeName()
                + " was given.");
      }
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    // We will be returning a Text object
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  } 

  /**
   * Get a parsed string from an input user agent string
   * 
   * @param UserAgent - string containing the user agent to parse
   * 
   * @param options - options from the set of strings "os", "device", and "ua". "os" and "ua" 
   *  may optionally append "_family", "_major" and "_minor". 
   *  "os" and "ua" return json; other options return a string only. 
   *  No option returns a JSON formatted string (example: "{user_agent: %s, os: %s, device: %s}")
   *
   * @return string containing a parsed user agent based upon options entered.
   *         string.
   */
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    assert (arguments.length>0 && arguments.length<3);
    Text UserAgent = (Text) converters[0].convert(arguments[0].get());
    Text options = (arguments.length == 2 ? (Text) converters[1].convert(arguments[1].get()) : null) ;

    if (UserAgent == null ) {
      return null;
    }

    try {
		Client c = uaParser.parse(UserAgent.toString());

		if (options == null) {
			result.set(c.toString());
		}

		else {
			userOptions uo = userOptions.valueOf(options.toString().toLowerCase());
			
			switch (uo)	{
				case os:
					result.set(c.os.toString());
					break;
				case os_family:
					result.set(c.os.family == null ? "null" : c.os.family );
					break;
				case os_major:
					result.set(c.os.major == null ? "null" : c.os.major );
					break;
				case os_minor:
					result.set(c.os.minor == null ? "null" : c.os.minor );
					break;
				case ua:
					result.set(c.userAgent.toString());
					break;
				case ua_family:
					result.set(c.userAgent.family == null ? "null" : c.userAgent.family );
					break;
				case ua_major:
					result.set(c.userAgent.major == null ? "null" : c.userAgent.major );
					break;
				case ua_minor:
					result.set(c.userAgent.minor == null ? "null" : c.userAgent.minor );
					break;
				case device:
					result.set(c.device.family == null ? "null" : c.device.family );
					break;
				default:
					result = null;
					break;
			}
        }
    } catch (IllegalArgumentException e) {
		LOG.warn("Caught IllegalArgumentException: " + e.getMessage());
		return null;
	}

	return result;
  }

//  public Text evaluate(Text UserAgent) {
//    return evaluate(UserAgent, null);
//  }
  @Override
  public String getDisplayString(String[] children) {
    assert (children.length > 0 && children.length < 3);
    return "user_agent_parser(" + children[0] + ( children.length == 1 ? "" : ", " + children[1] )  + ")";
  }
}
