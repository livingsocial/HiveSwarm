package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.udf.UDFLike;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

@description(
	     name = "ilike",
	     value = "_FUNC_(str, pattern) - Checks if str matches pattern",
	     extended = "Example:\n" +
	     "  > SELECT a.* FROM srcpart a WHERE a.hr _FUNC_ '%2' LIMIT 1;\n" +
	     "  27      val_27  2008-04-08      12"
	     )
public class ILike extends UDFLike {
    public BooleanWritable evaluate(Text s, Text likePattern) {
	if(s != null && likePattern != null) {
	    s.set(s.toString().toLowerCase());
	    likePattern.set(likePattern.toString().toLowerCase());
	}
	return super.evaluate(s, likePattern);
    }
}
