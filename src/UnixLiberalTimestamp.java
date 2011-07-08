package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.udf.UDFUnixTimeStamp;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@Description(
	     name = "unix_liberal_timestamp",
	     value = "_FUNC_(str) - gets unix timestamp in either yyyy-MM-dd HH:mm:ss or yyyy-MM-dd format",
	     extended = "Example:\n" +
	     "  > SELECT a.* FROM srcpart a WHERE _FUNC_ (a.hr) < unix_timestamp() LIMIT 1;\n"
	     )
public class UnixLiberalTimestamp extends UDFUnixTimeStamp {
    public LongWritable evaluate(Text datestring) {
	if(datestring.find(" ") == -1)
	    datestring = new Text(datestring.toString() + " 00:00:00");
	return super.evaluate(datestring);
    }
}
