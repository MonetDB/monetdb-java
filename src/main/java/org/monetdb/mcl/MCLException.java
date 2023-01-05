/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

package org.monetdb.mcl;

/**
 * A general purpose Exception class for MCL related problems.  This
 * class should be used if no more precise Exception class exists.
 */
public final class MCLException extends Exception {
	private static final long serialVersionUID = 1L;

	public MCLException(String e) {
		super(e);
	}
}
