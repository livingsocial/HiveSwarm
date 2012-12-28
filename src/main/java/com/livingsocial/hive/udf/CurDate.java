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
	name = "curdate", 
	value = "_FUNC_() - Returns current date in format 'yyyy-mm-dd'",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_() FROM src LIMIT 1;\n"
	     + "  2012-08-10\n"
)

public class CurDate extends UDF {

  /**
   * returns the current date in fomrat 'yyyy-mm-dd'
   * 
   * @return today's date in format 'yyyy-mm-dd'
   *         string.
   */

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

  private Text result = new Text();
  static final Log LOG = LogFactory.getLog(CurDate.class.getName());

//  private Text lastFormat = new Text();

//  private Text defaultFormat = new Text("yyyy-MM-dd");

  public Text evaluate() {
	Date date = new java.sql.Date(System.currentTimeMillis());
	result.set(formatter.format(date));
	return result;
  }

}
