package com.livingsocial.hive.udf;

import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.text.SimpleDateFormat;

import com.livingsocial.hive.Utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Description(
	name = "curdatetime", 
	value = "_FUNC_() - Returns current date/time in format 'yyyy-mm-dd HH:MM:SS'",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_() FROM src LIMIT 1;\n"
	     + "  2012-08-10 12:00:00\n"
)

public class CurDateTime extends UDF {

  /**
   * returns the current date/time in fomrat 'yyyy-mm-dd'
   * 
   * @return today's date in format 'yyyy-mm-dd'
   *         string.
   */

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private Text result = new Text();
  static final Log LOG = LogFactory.getLog(Curdate.class.getName());

  public Text evaluate() {
	Date date = new java.sql.Date(System.currentTimeMillis());
	result.set(formatter.format(date));
	return result;
  }

}
