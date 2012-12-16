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
    + "  > SELECT _FUNC_('Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3','os') FROM src LIMIT 1;\n"
         + "  iOS_5 \n")
public class UserAgentParser extends UDF {

  private Text result = new Text();
  static final Log LOG = LogFactory.getLog(UserAgentParser.class.getName());


  public UserAgentParser() {
  }

  /**
   * Get the day of week from a date string.
   * 
   * @param UserAgent - string containing the user agent to parse
   * 
   * @param options - options from the set of strings "os", "device", and "ua". No option 
   * 				returns a string in format "{user_agent: %s, os: %s, device: %s}"
   * @return string containing a parsed user agent based upon options entered.
   *         string.
   */
  public Text evaluate(Text UserAgent, Text options) {
    if (UserAgent == null) {
      return null;
    }
	LOG.warn("user agent: " + UserAgent.toString());
    try {
		LOG.warn("checkpoint 1");
		Parser uaParser = new Parser();
		LOG.warn("checkpoint 2");
		Client c = uaParser.parse(UserAgent.toString());
		LOG.warn("checkpoint 3");

		if (options == null) {
			result.set(c.toString());
		}
		else {
			if (options.toString().toLowerCase().equals("os")) {
				result.set(c.os.family + "_" + c.os.major);
			}
			else if (options.toString().toLowerCase().equals("device")) {
				result.set(new StringBuilder(c.device.family).toString());
			}
			else if (options.toString().toLowerCase().equals("ua")) {
				result.set(new StringBuilder(c.userAgent.family).append("_").append(c.userAgent.major).toString());
			}
			else 
				result = null;
        }
    } catch (IOException e) {
		LOG.warn("Caught IOException: " + e.getMessage());
		return null;
    } 
	return result;
  }
  public Text evaluate(Text UserAgent) {
    return evaluate(UserAgent, null);
  }

}
