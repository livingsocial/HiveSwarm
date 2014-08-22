package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;
import java.net.URLDecoder;

@Description(
	     name = "urldecode",
	     value = "_FUNC_(str) - Returns urldecoded string",
	     extended = "Example:\n" +
	     "  > SELECT urldecode(url_refer) FROM visits a;\n" +
	     "  http://www.livingsocial.com/Buy Now"
	     )
public class URLDecode extends UDF {
    public Text evaluate(Text s) {
	Text to_value new Text();
	if(s != null) {
	    try { 
		to_value.set(URLDecoder.decode(s.toString(), "UTF-8"));
	    } catch (Exception e) {
		to_value = s;
	    };
	}
	return to_value;
    }
}
