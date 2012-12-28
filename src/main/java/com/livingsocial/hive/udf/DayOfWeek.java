package com.livingsocial.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@Description(name = "dayofweek", 
	     value = "_FUNC_(date) - Returns the day of the week",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2011-08-29') FROM src LIMIT 1;\n"
	     + "  1\n")
public class DayOfWeek extends UDF {
  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private final Calendar calendar = Calendar.getInstance();

  private IntWritable result = new IntWritable();

  public DayOfWeek() {
    calendar.setFirstDayOfWeek(Calendar.MONDAY);
  }

  /**
   * Get the day of week from a date string.
   * 
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd"
   * @return an int from 1 to 7. null if the dateString is not a valid date
   *         string.
   */
  public IntWritable evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
      Date date = formatter.parse(dateString.toString());
      calendar.setTime(date);
      result.set(calendar.get(Calendar.DAY_OF_WEEK));
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

}
