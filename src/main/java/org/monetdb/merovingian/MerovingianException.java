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

package org.monetdb.merovingian;

/**
 * An Exception raised when monetdbd specific problems occur.
 *
 * This class is a shallow wrapper around Exception to identify an
 * exception as one originating from the monetdbd instance being
 * communicated with, instead of a locally generated one.
 *
 * @author Fabian Groffen
 * @version 1.0
 */
public class MerovingianException extends Exception {
	private static final long serialVersionUID = 101L;

	public MerovingianException(String reason) {
		super(reason);
	}
}
