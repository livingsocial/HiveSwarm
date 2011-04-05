package com.livingsocial.hive.udf;

import com.livingsocial.hive.Utils;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

// Strings *must* be either dates or date times.
public final class MaxDate extends UDF {
    public Text evaluate(final Text first, final Text second) {
	if(first == null) return second;
	if(second == null) return first;
	
	long firstmil = Utils.stringToTimestamp(first.toString());
	long secondmil = Utils.stringToTimestamp(second.toString());

	// if parsing both dates failed, return null
	if(firstmil < 0 && secondmil < 0)
	    return null;

	return (firstmil > secondmil) ? first : second;
    }

}