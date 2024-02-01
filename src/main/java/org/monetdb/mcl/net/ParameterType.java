/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

package org.monetdb.mcl.net;

/**
 * Enumeration of the types a {@link Parameter} may have.
 */
public enum ParameterType {
	/**
	 * The Parameter is an arbitrary string
	 */
	Str,
	/** The Parameter can be interpreted as an {@link Integer} */
	Int,
	/** The Parameter is a {@link Boolean} and can be
	 * written "true", "false", "on", "off", "yes" or "no".
	 * Uppercase letters are also accepted
	 */
	Bool,
	/**
	 * Functionally the same as {@link ParameterType#Str} but
	 * indicates the value is to be interpreted as a path on the
	 * client's file system.
	 */
	Path;

	/**
	 * Convert a string to a boolean, accepting true/false/yes/no/on/off.
	 * 
	 * Uppercase is also accepted.
	 *
	 * @param value text to be parsed
	 * @return boolean interpretation of the text
	 */
	public static boolean parseBool(String value) {
		boolean lowered = false;
		String original = value;
		while (true) {
			switch (value) {
				case "true":
				case "yes":
				case "on":
					return true;
				case "false":
				case "no":
				case "off":
					return false;
				default:
					if (!lowered) {
						value = value.toLowerCase();
						lowered = true;
						continue;
					}
					throw new IllegalArgumentException("invalid boolean value: " + original);
			}
		}
	}

	/**
	 * Convert text into an Object of the appropriate type
	 *
	 * @param name  name of the setting for use in error messages
	 * @param value text to be converted
	 * @return Object representation of the text
	 * @throws ValidationError if the text cannot be converted
	 */
	public Object parse(String name, String value) throws ValidationError {
		if (value == null)
			return null;

		try {
			switch (this) {
				case Bool:
					return parseBool(value);
				case Int:
					return Integer.parseInt(value);
				case Str:
				case Path:
					return value;
				default:
					throw new IllegalStateException("unreachable");
			}
		} catch (IllegalArgumentException e) {
			String message = e.toString();
			throw new ValidationError(name, message);
		}
	}

	/**
	 * Represent the object as a string.
	 *
	 * @param value, must be of the appropriate type
	 * @return textual representation
	 */
	public String format(Object value) {
		switch (this) {
			case Bool:
				return (Boolean) value ? "true" : "false";
			case Int:
				return Integer.toString((Integer) value);
			case Str:
			case Path:
				return (String) value;
			default:
				throw new IllegalStateException("unreachable");
		}
	}
}
