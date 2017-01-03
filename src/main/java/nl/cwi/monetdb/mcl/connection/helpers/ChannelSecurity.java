/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection.helpers;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class ChannelSecurity {

    private static char HexChar(int n) { return (n > 9) ? (char) ('a' + (n - 10)) : (char) ('0' + n); }

    /**
     * Small helper method to convert a byte string to a hexadecimalstring representation.
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

    public static String DigestStrings(String algorithm, byte[]... toDigests) {
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            for (byte[] str : toDigests) {
                md.update(str);
            }
            return ToHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("internal error: " + e.toString());
        }
    }
}
