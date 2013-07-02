package com.livingsocial.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * 
 * UDFQuarter
 *
 * From https://issues.apache.org/jira/browse/HIVE-3404 until it's in the main hive build
 * 
 */
@Description(name = "quarter", value = "_FUNC_(date or timestamp) -" +
		" Returns the quarter of the year corresponding to date or timestamp")
public class UDFQuarter extends UDF {
	private final SimpleDateFormat formatter = new SimpleDateFormat(
			"yyyy-MM-dd");
	private final Calendar calendar = Calendar.getInstance();
	private IntWritable result = new IntWritable();

	public UDFQuarter() {

	}
	 /**
	   * Get the quarter of the  year from a date string.
	   *
	   * @param dateString
	   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
	   *          "yyyy-MM-dd".
	   * @return an IntWritable from 1 to 4. null if the dateString is not a valid date
	   *         string.
	   */
	public IntWritable evaluate(Text dateString) {
		if (dateString == null) {
			return null;
		}
		try {
			Date date = formatter.parse(dateString.toString());
			calendar.setTime(date);
			int month = calendar.get(Calendar.MONTH) + 1;
			result = getQuarter(month);

		} catch (ParseException e) {
			return null;
		}
		return result;

	}
	/**
	   * Get the quarter of the  year from a date Timestamp.
	   *
	   * @param dateTimeStamp
	   *          the dateTimeStamp in the format of "yyyy-MM-dd HH:mm:ss" or
	   *          "yyyy-MM-dd".
	   * @return an IntWritable from 1 to 4. null if the dateTimeStamp is not a valid date
	   *         TimeStamp.
	   */
	public IntWritable evaluate(TimestampWritable dateTimeStamp) {
		if (dateTimeStamp == null) {
			return null;
		}
		calendar.setTime(dateTimeStamp.getTimestamp());
		int month = calendar.get(Calendar.MONTH) + 1;
		result = getQuarter(month);
		return result;
	}

	/**
	 * getQuarter method calculates in which quarter the date falls
	 * 
	 * @param monthOfYear
	 * @return quarter
	 */
	public IntWritable getQuarter(int monthOfYear) {
		IntWritable quarter = new IntWritable();

		if (monthOfYear >= 1 && monthOfYear <= 3) {
			quarter.set(1);
		} else if (monthOfYear >= 4 && monthOfYear <= 6) {
			quarter.set(2);
		} else if (monthOfYear >= 7 && monthOfYear <= 9) {
			quarter.set(3);
		} else if (monthOfYear >= 10 && monthOfYear <= 12) {
			quarter.set(4);
		}
		return quarter;

	}
}
