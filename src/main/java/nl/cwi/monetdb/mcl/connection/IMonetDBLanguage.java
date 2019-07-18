/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection;

/**
 * An interface which represents the delimiters for user queries depending on the language (SQL and MAL) and connection
 * (Socket and Embedded).
 *
 * @author Pedro Ferreira
 */
public interface IMonetDBLanguage {

	/**
	 * Gets the String representation of a query delimiter represented through the index parameter.
	 *
	 * @param index The delimiter index starting from 0
	 * @return The String representation of the delimiter
	 */
	String getQueryTemplateIndex(int index);

	/**
	 * Gets the String representation of a command delimiter represented through the index parameter.
	 *
	 * @param index The delimiter index starting from 0
	 * @return The String representation of the delimiter
	 */
	String getCommandTemplateIndex(int index);

	/**
	 * Gets all query delimiters.
	 *
	 * @return All query delimiters
	 */
	String[] getQueryTemplates();

	/**
	 * Gets all command delimiters.
	 *
	 * @return All command delimiters
	 */
	String[] getCommandTemplates();

	/**
	 * Gets the String representation of the language currently used.
	 *
	 * @return The String representation of the language currently used.
	 */
	String getRepresentation();
}
