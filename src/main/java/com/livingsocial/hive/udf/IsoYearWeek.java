package com.livingsocial.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
// import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * IsoYearWeek.
 *
 */
@Description(name = "isoyearweek",
    value = "_FUNC_(date) - Returns the year of ISO week number of the given date. A week "
    + "is considered to start on a Monday and week 1 is the first week with >3 days."
	+ "The first 1-3 days of a year prior to a week ",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2008-02-20') FROM src LIMIT 1;\n"
    + "  8\n"
    + "  > SELECT _FUNC_('1980-12-31 12:59:59') FROM src LIMIT 1;\n" + "  1")
public class IsoYearWeek extends UDF {
  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private final Calendar cal = Calendar.getInstance();
  private final Calendar calNearThur = Calendar.getInstance();

  private IntWritable result = new IntWritable();

  public IsoYearWeek() {
    cal.setFirstDayOfWeek(Calendar.MONDAY);
    cal.setMinimalDaysInFirstWeek(4);
    calNearThur.setFirstDayOfWeek(Calendar.MONDAY);
    calNearThur.setMinimalDaysInFirstWeek(4);
  }

  /**
   * Get the week of the year from a date string.
   *
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return an int from 1 to 53. null if the dateString is not a valid date
   *         string.
   */
  public IntWritable evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
		Date date = formatter.parse(dateString.toString());
		int checkYear;
		
		cal.setTime(date);
		calNearThur.setTime(date);
	
		boolean isSunday = cal.get(Calendar.DAY_OF_WEEK)==Calendar.SUNDAY;
		if (isSunday)
		    calNearThur.add(Calendar.DAY_OF_MONTH,5-(cal.get(Calendar.DAY_OF_WEEK)+7));
		else
		    calNearThur.add(Calendar.DAY_OF_MONTH,5-(cal.get(Calendar.DAY_OF_WEEK)));
		result.set(calNearThur.get(Calendar.YEAR));
		return (result);
    } catch (ParseException e) {
      return null;
    }
  }

//  public IntWritable evaluate(TimestampWritable t) {
//    if (t == null) {
//      return null;
//    }
//
//    cal.setTime(t.getTimestamp());
//    calNearThur.setTime(t.getTimestamp());
//	
//	boolean isSunday = cal.get(Calendar.DAY_OF_WEEK)==Calendar.SUNDAY;
//	if (isSunday)
//	    calNearThur.add(Calendar.DAY_OF_MONTH,5-(cal.get(Calendar.DAY_OF_WEEK)+7));
//	else
//	    calNearThur.add(Calendar.DAY_OF_MONTH,5-(cal.get(Calendar.DAY_OF_WEEK)));
//	result.set(calNearThur.get(Calendar.YEAR));
//	return (result);
//  }

}
