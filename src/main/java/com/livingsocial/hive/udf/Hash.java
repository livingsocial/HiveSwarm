package com.livingsocial.hive.udf;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(
    name = "ls_hash",
    value = "_FUNC_(some_id, [some_salt, ['debug']]) - Generate a consistent hash based 'random' number between 0 and 1 mixing in the id and salt.",
    extended = "Generates a consistent 'random' number based on the passed in id and salt. \n" +
               " Normally this would be used to create a percentage based sample of a group with a query like:\n" +
    		       "  select * from some_table_to_sample where _FUNC_(id, 'my_salt') < 0.10;  -- extract a 10% random sample"
    )
public final class Hash extends UDF {

	private static final Text EMPTY = new Text("");
	private static final int  CHARS_TO_USE = 14;
	private static final double MAX_SIZE = Math.pow(2, CHARS_TO_USE*4);
	
	private Map<String,Object> mapOut = new HashMap<String,Object>();
	private Charset charset = Charset.forName("UTF8");
			
	public Double evaluate(final Text id) {
		return evaluate(id, EMPTY);
	}

	public Double evaluate(Text id, Text salt) {
		Double tmp = (Double) hashIt(id, salt).get("output");
		return tmp;
	}
	

	public Text evaluate(Text id, Text salt, Text debug) {
		Map<String,Object> map = hashIt(id, salt);
		StringBuilder builder = new StringBuilder();
		for(Map.Entry<String,Object> entry: (Collection<Map.Entry<String,Object>>)map.entrySet()) {
			builder.append(entry.getKey() + "=" + entry.getValue() + ",");
		}
		return new Text(builder.toString());
	}
	
	private Map<String,Object> hashIt(Text id, Text salt) {
		mapOut.clear();
		if (id == null) {
			return mapOut;
		}
		
		if(salt == null) {
			salt = EMPTY;
		}
		
		String toHash = id.toString() + salt;
		
		try {
			MessageDigest md = MessageDigest.getInstance("SHA1");
			md.update(toHash.getBytes(charset));
			byte[] hash = md.digest();
			String hexHash = Hex.encodeHexString(hash);
			String finalHash = hexHash.substring(0, CHARS_TO_USE);
			long hashNum = Long.parseLong(finalHash, 16);
			double value = hashNum / MAX_SIZE;
			
			// Slightly hacky way to expose the internals of this
			mapOut.put("hexHash", hexHash);
			mapOut.put("finalHash", finalHash);
			mapOut.put("hashNum", hashNum);
			mapOut.put("toHash", toHash);
			mapOut.put("output", value);
			
			return mapOut;
		} catch (NoSuchAlgorithmException nsae) {
			throw new IllegalArgumentException("SHA1 is not setup");
		}
	}
}
