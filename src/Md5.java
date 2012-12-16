package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.security.*;

/**
 * Fork of datamine md5 
 * originally found at https://gist.github.com/1050002
 */
public final class Md5 extends UDF {

	public Text evaluate(final Text s) {
	    if (s == null) {
                return null;
	    }
	    try {
	    	MessageDigest md = MessageDigest.getInstance("MD5");
	    	md.update(s.toString().getBytes());
	    	byte[] md5hash = md.digest();
	    	StringBuilder builder = new StringBuilder();
	    	for (byte b : md5hash) {
	    	    builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
	    	}
		return new Text(builder.toString());
	    } catch (NoSuchAlgorithmException nsae) {
	    	System.out.println("Cannot find digest algorithm");
                System.exit(1);
	    }
	    return null;
	}
}
