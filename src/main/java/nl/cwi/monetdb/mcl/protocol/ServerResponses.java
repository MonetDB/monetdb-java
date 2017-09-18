/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

/**
 * This class represents the possible stages of a query response by the server.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public final class ServerResponses {

	private ServerResponses() {}

	/* Please don't change the order or the values */

	/** "there is currently no line", or the the type is unknown is represented by UNKNOWN */
	public static final int UNKNOWN = 0;
	/** a line starting with ! indicates ERROR */
	public static final int ERROR = 1;
	/** a line starting with % indicates HEADER */
	public static final int HEADER = 2;
	/** a line starting with [ indicates RESULT */
	public static final int RESULT = 3;
	/** a line which matches the pattern of prompt1 is a PROMPT */
	public static final int PROMPT = 4;
	/** a line which matches the pattern of prompt2 is a MORE */
	public static final int MORE = 5;
	/** a line starting with &amp; indicates the start of a header block */
	public static final int SOHEADER = 6;
	/** a line starting with ^ indicates REDIRECT */
	public static final int REDIRECT = 7;
	/** a line starting with # indicates INFO */
	public static final int INFO = 8;
}
