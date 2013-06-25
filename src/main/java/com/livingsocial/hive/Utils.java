package com.livingsocial.hive;

import java.text.ParseException;
import java.text.SimpleDateFormat;


public class Utils {

    private Utils() {}
    
    // Returns -1 if date can't be parsed
    public static long stringToTimestamp(String date) {
	long time = -1L;
	if(date.indexOf(" ") == -1) {
	    try {
		time = (new SimpleDateFormat("yyyy-MM-dd")).parse(date).getTime() / 1000;
	    } catch(ParseException pe) {}
	} else {
	    try {
		time = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(date).getTime() / 1000;
	    } catch(ParseException pe) {}
	}
	return time;
    }
}
