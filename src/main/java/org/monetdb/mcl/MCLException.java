/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

package org.monetdb.mcl;

/**
 * A general purpose Exception class for MCL related problems.  This
 * class should be used if no more precise Exception class exists.
 */
@SuppressWarnings("serial")
public final class MCLException extends Exception {
	public MCLException(String message) {
		super(message);
	}

	public MCLException(String message, Exception cause) {
		super(message, cause);
	}
}
