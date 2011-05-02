package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.Text;
import java.net.URLDecoder;

@description(
	     name = "urldecode",
	     value = "_FUNC_(str) - Returns urldecoded string",
	     extended = "Example:\n" +
	     "  > SELECT urldecode(url_refer) FROM visits a;\n" +
	     "  http://www.livingsocial.com/Buy Now"
	     )
public class URLDecode extends UDF {
    public Text evaluate(Text s) {
	Text to_value = new Text(s);
	if(s != null) {
	    try { 
		to_value.set(URLDecoder.decode(s.toString(), "UTF-8"));
	    } catch (Exception e) {};
	}
	return to_value;
    }
}