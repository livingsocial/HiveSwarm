package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.security.*;

/**
 * Fork of datamine md5 changing it to sha1
 * originally found at https://gist.github.com/1050002
 */
public final class Sha1 extends UDF {

	public Text evaluate(final Text s) {
	    if (s == null) {
                return null;
	    }
	    try {
	    	MessageDigest md = MessageDigest.getInstance("SHA1");
	    	md.update(s.toString().getBytes());
	    	byte[] hash = md.digest();
	    	StringBuilder builder = new StringBuilder();
	    	for (byte b : hash) {
	    	    builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
	    	}
		return new Text(builder.toString());
	    } catch (NoSuchAlgorithmException nsae) {
	    	throw new IllegalArgumentException("SHA1 is not setup");
	    }
	}
}
