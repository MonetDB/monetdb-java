package nl.cwi.monetdb.mcl.protocol.oldmapi;

final class OldMapiTupleLineParserHelper {

    static int CharIndexOf(char[] source, int sourceCount, char[] target, int targetCount) {
        if (targetCount == 0) {
            return 0;
        }

        char first = target[0];
        int max = sourceCount - targetCount;

        for (int i = 0; i <= max; i++) {
            /* Look for first character. */
            if (source[i] != first) {
                while (++i <= max && source[i] != first);
            }

            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = 1; j < end && source[j] == target[k]; j++, k++);

                if (j == end) {
                    /* Found whole string. */
                    return i;
                }
            }
        }
        return -1;
    }

    private static final char[] TrueConstant = new char[]{'t','r','u','e'};

    static byte CharArrayToBoolean(char[] data, int start, int count) {
        return CharIndexOf(data, start + count, TrueConstant, 4) == start ? (byte)1 : (byte)0;
    }

    static byte CharArrayToByte(char[] data, int start, int count) {
        byte tmp = 0;
        int limit = start + count;
        boolean positive = true;
        char chr = data[start++];
        // note: don't use Character.isDigit() here, because we only want ISO-LATIN-1 digits
        if (chr >= '0' && chr <= '9') {
            tmp = (byte)(chr - '0');
        } else if(chr == '-') {
            positive = false;
        } else {
            throw new NumberFormatException();
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
                throw new NumberFormatException();
            }
        }
        return positive ? tmp : (byte) -tmp;
    }

    static short CharArrayToShort(char[] data, int start, int count) {
        short tmp = 0;
        int limit = start + count;
        boolean positive = true;
        char chr = data[start++];
        // note: don't use Character.isDigit() here, because we only want ISO-LATIN-1 digits
        if (chr >= '0' && chr <= '9') {
            tmp = (short)(chr - '0');
        } else if(chr == '-') {
            positive = false;
        } else {
            throw new NumberFormatException();
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
                throw new NumberFormatException();
            }
        }
        return positive ? tmp : (short) -tmp;
    }

    static int CharArrayToInt(char[] data, int start, int count) {
        int tmp = 0, limit = start + count;
        boolean positive = true;
        char chr = data[start++];
        // note: don't use Character.isDigit() here, because we only want ISO-LATIN-1 digits
        if (chr >= '0' && chr <= '9') {
            tmp = chr - '0';
        } else if(chr == '-') {
            positive = false;
        } else {
            throw new NumberFormatException();
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
                throw new NumberFormatException();
            }
        }
        return positive ? tmp : -tmp;
    }

    static long CharArrayToLong(char[] data, int start, int count) {
        long tmp = 0;
        int limit = start + count;
        boolean positive = true;
        char chr = data[start++];
        // note: don't use Character.isDigit() here, because we only want ISO-LATIN-1 digits
        if (chr >= '0' && chr <= '9') {
            tmp = chr - '0';
        } else if(chr == '-') {
            positive = false;
        } else {
            throw new NumberFormatException();
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
                throw new NumberFormatException();
            }
        }
        return positive ? tmp : -tmp;
    }
}
