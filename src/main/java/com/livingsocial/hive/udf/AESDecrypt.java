package com.livingsocial.hive.udf;

import com.livingsocial.hive.Base32;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

import javax.crypto.spec.SecretKeySpec;
import javax.crypto.Cipher;


@Description(
	     name = "aesdecrypt",
	     value = "_FUNC_(str, key) - Returns unencrypted string based on AES key.  Str must be base32hex encoded.",
	     extended = "Example:\n" +
	     "  > SELECT aesdecrypt(credit_card_number, 'asd0fjas0df9asjfd09asjdf') FROM credit_cards;\n" +
	     "  123456789456468466"
	     )
public class AESDecrypt extends UDF {
    public Text evaluate(Text encrypted, Text key) {
	Text unencrypted = new Text(encrypted);
	if(encrypted != null && key != null) {
	    try {
		SecretKeySpec skeySpec = new SecretKeySpec(key.toString().getBytes(), "AES");
		Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");
		cipher.init(Cipher.DECRYPT_MODE, skeySpec);
		byte[] original = cipher.doFinal(Base32.hexdecode(encrypted.toString()));
		unencrypted.set(original);
	    } catch (Exception e) {};
	}
	return unencrypted;
    }
}