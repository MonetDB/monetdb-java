/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.ProtocolException;

/**
 * This is a helper Class for the OldMapiTupleLineParser. The main objective of this class is to parse primitive types
 * without any memory allocation for performance reasons. The code may seem to be boilerplate, but it has to be done
 * this way to due to poor typing of the Java programming language.
 *
 * @author Pedro Ferreira
 */
final class OldMapiTupleLineParserHelper {

	private OldMapiTupleLineParserHelper() {}

	/**
	 * Checks if a char[] (target) is inside on another (source), retrieving the first index on the source, where the
	 * target is, if found. In other words this a Java implementation of the strstr function from the C standard.
	 * As we search always from the beginning of the source, the start parameter is not used.
	 *
	 * @param source The source char[] to search
	 * @param sourceCount The number of characters in the source array to search
	 * @param target The target char[] to be found
	 * @param targetCount The result set column SQL types
	 * @return The integer representation of the Table Result Header retrieved
	 */
	static int charIndexOf(char[] source, int sourceOffset, int sourceCount, char[] target, int targetOffset,
						   int targetCount, int fromIndex) {
		if (fromIndex >= sourceCount) {
			return (targetCount == 0 ? sourceCount : -1);
		}
		if (fromIndex < 0) {
			fromIndex = 0;
		}
		if (targetCount == 0) {
			return fromIndex;
		}

		char first = target[targetOffset];
		int max = sourceOffset + (sourceCount - targetCount);

		for (int i = sourceOffset + fromIndex; i <= max; i++) {
			/* Look for first character. */
			if (source[i] != first) {
				while (++i <= max && source[i] != first);
			}
			if (i <= max) {
				int j = i + 1;
				int end = j + targetCount - 1;
				for (int k = targetOffset + 1; j < end && source[j] == target[k]; j++, k++);

				if (j == end) {
					/* Found whole string. */
					return i - sourceOffset;
				}
			}
		}
		return -1;
	}

	/**
	 * The character array representation of a TRUE value.
	 */
	private static final char[] TRUE_CONSTANT = new char[]{'t','r','u','e'};

	/**
	 * Converts a segment of a CharBuffer's backing array into a Java boolean.
	 *
	 * @param start The first position in the array to parse
	 * @param data The CharBuffer's backing array to parse
	 * @return 1 it's a true value, 0 if false
	 */
	static byte charArrayToBoolean(char[] data, int start) {
		return charIndexOf(data, 0, data.length, TRUE_CONSTANT, 0, 4, start)
				== start ? (byte)1 : (byte)0;
	}

	/**
	 * Converts a segment of a CharBuffer's backing array into a Java byte.
	 *
	 * @param data The CharBuffer's backing array to parse
	 * @param start The first position in the array to parse
	 * @param count The number of characters to read from the starter position
	 * @return The parsed byte value
	 */
	static byte charArrayToByte(char[] data, int start, int count) throws ProtocolException {
		byte tmp = 0;
		int limit = start + count;
		boolean positive = true;
		char chr = data[start++];

		if (chr >= '0' && chr <= '9') {
			tmp = (byte)(chr - '0');
		} else if(chr == '-') {
			positive = false;
		} else {
			throw new ProtocolException("Expected a digit at the position " + (start - 1));
		}
		while (start < limit) {
			chr = data[start++];
			if(chr == ' ') {
				break;
			}
			tmp *= 10;
			if (chr >= '0' && chr <= '9') {
				tmp += chr - '0';
			} else {
				throw new ProtocolException("Expected a digit at the position " + (start - 1));
			}
		}
		return positive ? tmp : (byte) -tmp;
	}

	/**
	 * Converts a segment of a CharBuffer's backing array into a Java short.
	 *
	 * @param data The CharBuffer's backing array to parse
	 * @param start The first position in the array to parse
	 * @param count The number of characters to read from the starter position
	 * @return The parsed short value
	 */
	static short charArrayToShort(char[] data, int start, int count) throws ProtocolException {
		short tmp = 0;
		int limit = start + count;
		boolean positive = true;
		char chr = data[start++];

		if (chr >= '0' && chr <= '9') {
			tmp = (short)(chr - '0');
		} else if(chr == '-') {
			positive = false;
		} else {
			throw new ProtocolException("Expected a digit at the position " + (start - 1));
		}
		while (start < limit) {
			chr = data[start++];
			if(chr == ' ') {
				break;
			}
			tmp *= 10;
			if (chr >= '0' && chr <= '9') {
				tmp += chr - '0';
			} else {
				throw new ProtocolException("Expected a digit at the position " + (start - 1));
			}
		}
		return positive ? tmp : (short) -tmp;
	}

	/**
	 * Converts a segment of a CharBuffer's backing array into a Java int.
	 *
	 * @param data The CharBuffer's backing array to parse
	 * @param start The first position in the array to parse
	 * @param count The number of characters to read from the starter position
	 * @return The parsed int value
	 */
	static int charArrayToInt(char[] data, int start, int count) throws ProtocolException {
		int tmp = 0, limit = start + count;
		boolean positive = true;
		char chr = data[start++];

		if (chr >= '0' && chr <= '9') {
			tmp = chr - '0';
		} else if(chr == '-') {
			positive = false;
		} else {
			throw new ProtocolException("Expected a digit at the position " + (start - 1));
		}
		while (start < limit) {
			chr = data[start++];
			if(chr == ' ') {
				break;
			} else if(chr == '.') { //for intervals
				continue;
			}
			tmp *= 10;
			if (chr >= '0' && chr <= '9') {
				tmp += (int)chr - (int)'0';
			} else {
				throw new ProtocolException("Expected a digit at the position " + (start - 1));
			}
		}
		return positive ? tmp : -tmp;
	}

	/**
	 * Converts a segment of a CharBuffer's backing array into a Java long.
	 *
	 * @param data The CharBuffer's backing array to parse
	 * @param start The first position in the array to parse
	 * @param count The number of characters to read from the starter position
	 * @return The parsed long value
	 */
	static long charArrayToLong(char[] data, int start, int count) throws ProtocolException {
		long tmp = 0;
		int limit = start + count;
		boolean positive = true;
		char chr = data[start++];

		if (chr >= '0' && chr <= '9') {
			tmp = chr - '0';
		} else if(chr == '-') {
			positive = false;
		} else {
			throw new ProtocolException("Expected a digit at the position " + (start - 1));
		}
		while (start < limit) {
			chr = data[start++];
			if(chr == ' ') {
				break;
			} else if(chr == '.') { //for intervals
				continue;
			}
			tmp *= 10;
			if (chr >= '0' && chr <= '9') {
				tmp += chr - '0';
			} else {
				throw new ProtocolException("Expected a digit at the position " + (start - 1));
			}
		}
		return positive ? tmp : -tmp;
	}
}
