package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.jsoup.Jsoup;


@Description(
	     name = "striphtml",
	     value = "_FUNC_(str) - Returns str with all HTML tags removed."
	     )
public class StripHTML extends UDF {
    public Text evaluate(Text html) {
    	String stripped = Jsoup.parse(html.toString()).text();
    	return new Text(stripped);
    }
}