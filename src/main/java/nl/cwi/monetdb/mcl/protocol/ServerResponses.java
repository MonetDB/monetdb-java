/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

public enum ServerResponses {

    /* Please don't change the order */

    /** "there is currently no line", or the the type is unknown is represented by UNKNOWN */
    UNKNOWN,
    /** a line starting with ! indicates ERROR */
    ERROR,
    /** a line starting with % indicates HEADER */
    HEADER,
    /** a line starting with [ indicates RESULT */
    RESULT,
    /** a line which matches the pattern of prompt1 is a PROMPT */
    PROMPT,
    /** a line which matches the pattern of prompt2 is a MORE */
    MORE,
    /** a line starting with &amp; indicates the start of a header block */
    SOHEADER,
    /** a line starting with ^ indicates REDIRECT */
    REDIRECT,
    /** a line starting with # indicates INFO */
    INFO
}
