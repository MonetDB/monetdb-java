/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection.helpers;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A helper class to process Hash digests during the authentication process.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public final class ChannelSecurity {

    private static char HexChar(int n) { return (n > 9) ? (char) ('a' + (n - 10)) : (char) ('0' + n); }

    /**
     * Helper method to convert a byte string to a hexadecimal String representation.
     *
     * @param digest The byte array to convert
     * @return The byte array as a hexadecimal string
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

    /**
     * Digests several byte[] into a String digest, using a specified hash algorithm.
     *
     * @param algorithm The hash algorithm to use
     * @param toDigests The Strings to digest
     * @return The Strings digest as a hexadecimal string
     */
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
