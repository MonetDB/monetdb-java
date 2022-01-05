/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

package org.monetdb.mcl.net;

/** Keep track of MAPI handshake options.
 *
 * Recent server versions (from 2021) allow you to send configuration information during
 * the authentication handshake so no additional round trips are necessary
 * when that has completed.
 *
 * This class keeps track of the options to send, and whether they have already
 * been sent.
 */
final public class HandshakeOptions {

	// public Boolean autoCommit;
	int replySize;
	// public Integer ColumnarProtocol;
	int timeZone;

	boolean mustSendReplySize;
	boolean mustSendTimeZone;

	public int getReplySize() {
		return replySize;
	}

	public void setReplySize(int replySize) {
		this.replySize = replySize;
		this.mustSendReplySize = true;
	}

	public boolean mustSendReplySize() {
		return mustSendReplySize;
	}

	public void mustSendReplySize(boolean mustSendReplySize) {
		this.mustSendReplySize = mustSendReplySize;
	}

	public int getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(int timeZone) {
		this.timeZone = timeZone;
		this.mustSendTimeZone = true;
	}

	public boolean mustSendTimeZone() {
		return mustSendTimeZone;
	}

	public void mustSendTimeZone(boolean mustSendTimeZone) {
		this.mustSendTimeZone = mustSendTimeZone;
	}

	public String formatResponse(int serverLevel) {
		StringBuilder opts = new StringBuilder(100);
		if (mustSendReplySize()) {
			formatOption(opts, Level.ReplySize, serverLevel, replySize);
			mustSendReplySize(false);
		}
		if (mustSendTimeZone()) {
			formatOption(opts, Level.TimeZone, serverLevel, timeZone);
			mustSendTimeZone(false);
		}

		return opts.toString();
	}

	private void formatOption(StringBuilder opts, Level level, int serverLevel, int value) {
		if (!level.isSupported(serverLevel))
			return;
		if (opts.length() > 0) {
			opts.append(",");
		}
		opts.append(level.field);
		opts.append("=");
		opts.append(value);
	}

	public enum Level {
		ReplySize("reply_size", 2),
		TimeZone("time_zone", 5);

		private final int level;
		private final String field;

		Level(String field, int level) {
			this.field = field;
			this.level = level;
		}

		public boolean isSupported(int serverLevel) {
			return this.level < serverLevel;
		}
	}
}
