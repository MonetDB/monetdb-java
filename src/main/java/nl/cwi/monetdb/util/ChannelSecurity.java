package nl.cwi.monetdb.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by ferreira on 12/1/16.
 */
public class ChannelSecurity {

    private static char HexChar(int n) { return (n > 9) ? (char) ('a' + (n - 10)) : (char) ('0' + n); }

    /**
     * Small helper method to convert a byte string to a hexadecimal
     * string representation.
     *
     * @param digest the byte array to convert
     * @return the byte array as hexadecimal string
     */
    private static String ToHex(byte[] digest) {
        char[] result = new char[digest.length * 2];
        int pos = 0;
        for (byte aDigest : digest) {
            result[pos++] = HexChar((aDigest & 0xf0) >> 4);
            result[pos++] = HexChar(aDigest & 0x0f);
        }
        return new String(result);
    }

    public static String DigestStrings(String algorithm, String... toDigests) {
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            for (String str : toDigests) {
                md.update(str.getBytes("UTF-8"));
            }
            byte[] digest = md.digest();
            return ToHex(digest);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new AssertionError("internal error: " + e.toString());
        }
    }
}
