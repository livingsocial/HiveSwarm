package com.livingsocial.hive.udf;

import java.text.ParseException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import ua_parser.Parser;
import ua_parser.Client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Description(name = "user_agent_parser",
         value = "_FUNC_(string, string) - returns parsed information about a user agent string",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3','os_major') FROM src LIMIT 1;\n"
         + "  iOS 5 \n")
public class UserAgentParser extends UDF {

  private Text result = new Text();
  static final Log LOG = LogFactory.getLog(UserAgentParser.class.getName());
  

  private enum userOptions {
	os, os_family, os_major, os_minor, ua, ua_family, ua_major, ua_minor, device
  }

  public UserAgentParser() {
  }

  /**
   * Get the day of week from a date string.
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
  public Text evaluate(Text UserAgent, Text options) {
    if (UserAgent == null) {
      return null;
    }
    try {
		Parser uaParser = new Parser();
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
					LOG.warn("value of device: " + c.device.toString());
					result.set(c.device.family == null ? "null" : c.device.family );
					break;
				default:
					result = null;
					break;
			}
        }
    } catch (IOException e) {
		LOG.warn("Caught IOException: " + e.getMessage());
		return null;
    } catch (IllegalArgumentException e) {
		LOG.warn("Caught IllegalArgumentException: " + e.getMessage());
		return null;
	}
	return result;
  }
  public Text evaluate(Text UserAgent) {
    return evaluate(UserAgent, null);
  }

}
