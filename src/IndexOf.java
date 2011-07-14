package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

@Description(
	     name = "index_of",
	     value = "_FUNC_(needle, haystack, [fromIndex]) - Returns first index of needle in haystack (optionally, after fromIndex) or -1 if not found",
	     extended = "Example:\n" +
	     "  > SELECT index_of('-', phone_number) FROM contact_info;\n" +
	     "  3"
	     )
public class IndexOf extends UDF {
    private final IntWritable result = new IntWritable();

    public IntWritable evaluate(Text needle, Text haystack, IntWritable fromIndex) {
	int index = 0;
	if(needle != null && haystack != null) 
	    index = haystack.toString().indexOf(needle.toString(), fromIndex.get());
	result.set(index);		
	return result;
    }

    public IntWritable evaluate(Text needle, Text haystack) {
	return evaluate(needle, haystack, new IntWritable(0));
    }
}