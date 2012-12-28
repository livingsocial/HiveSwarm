/* Adapted from:
 * (PD) 2001 The Bitzi Corporation
 * Please see http://bitzi.com/publicdomain for more info.
 */

package com.livingsocial.hive;

public class Base32 {
    private static final String base32Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    private static final int[] base32Lookup = { 
	0xFF,0xFF,0x1A,0x1B,0x1C,0x1D,0x1E,0x1F, // '0', '1', '2', '3', '4', '5', '6', '7'
	0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF, // '8', '9', ':', ';', '<', '=', '>', '?'
	0xFF,0x00,0x01,0x02,0x03,0x04,0x05,0x06, // '@', 'A', 'B', 'C', 'D', 'E', 'F', 'G'
	0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E, // 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O'
	0x0F,0x10,0x11,0x12,0x13,0x14,0x15,0x16, // 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W'
	0x17,0x18,0x19,0xFF,0xFF,0xFF,0xFF,0xFF, // 'X', 'Y', 'Z', '[', '\', ']', '^', '_'
	0xFF,0x00,0x01,0x02,0x03,0x04,0x05,0x06, // '`', 'a', 'b', 'c', 'd', 'e', 'f', 'g'
	0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E, // 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o'
	0x0F,0x10,0x11,0x12,0x13,0x14,0x15,0x16, // 'p', 'q', 'r', 's', 't', 'u', 'v', 'w'
	0x17,0x18,0x19,0xFF,0xFF,0xFF,0xFF,0xFF  // 'x', 'y', 'z', '{', '|', '}', '~', 'DEL'
    };

    static public byte[] decode(final String base32) {
        int i, index, lookup, offset, digit;
        byte[] bytes = new byte[base32.length() * 5 / 8];

        for(i = 0, index = 0, offset = 0; i < base32.length(); i++) {
            lookup = base32.charAt(i) - '0';

            /* Skip chars outside the lookup table */
            if ( lookup < 0 || lookup >= base32Lookup.length) {
                continue;
	    }

            digit = base32Lookup[lookup];

            /* If this digit is not in the table, ignore it */
            if (digit == 0xFF) {
                continue;
	    }

            if (index <= 3) {
                index = (index + 5) % 8;
                if (index == 0) {
                   bytes[offset] |= digit;
                   offset++;
                   if(offset>=bytes.length) break;
                } else {
                   bytes[offset] |= digit << (8 - index);
		}
            } else {
                index = (index + 5) % 8;
                bytes[offset] |= (digit >>> index);
                offset++;

                if(offset>=bytes.length) break;
                bytes[offset] |= digit << (8 - index);
            }
        }
	return bytes;
    }


    static public byte[] hexdecode(final String base32hex) {
	String values = "0123456789ABCDEFGHIJKLMNOPQRSTUV";
	String hexvalues = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
	String base32 = "";
	String base32hexuc = base32hex.toUpperCase();
	for(int i=0; i<base32hex.length(); i++) {
	    int index = values.indexOf(base32hexuc.charAt(i));
	    if(index > -1)
		base32 += Character.toString(hexvalues.charAt(index));
	}
	return decode(base32);
    }
}