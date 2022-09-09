/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

package org.monetdb.mcl.net;

import java.util.HashMap;
import java.util.Map;

/** Keep track of MAPI handshake options.
 *
 * Recent server versions (from 2021) allow you to send configuration information during
 * the authentication handshake so no additional round trips are necessary
 * when that has completed.
 *
 * This class keeps track of the values themselves, and also of whether or not they should still be sent.
 */
final public class HandshakeOptions {
	HashMap<Setting,Integer> options = new HashMap<>();
	int handshakeLevel = 0;

	public void set(Setting setting, int value) {
		options.put(setting, value);
	}

	public Integer get(Setting setting) {
		return options.get(setting);
	}

	public boolean wasSentInHandshake(Setting setting) {
		return setting.isSupported(this.handshakeLevel);
	}

	public boolean mustSend(Setting setting) {
		if (wasSentInHandshake(setting)) {
			return false;
		}
		Integer value = options.get(setting);
		return value != null && value != setting.defaultValue;
	}

	public String formatHandshakeResponse(int serverLevel) {
		StringBuilder opts = new StringBuilder(100);

		for (Map.Entry<Setting, Integer> entry: options.entrySet()) {
			Setting setting = entry.getKey();
			Integer value = entry.getValue();
			if (setting.isSupported(serverLevel)) {
				if (opts.length() > 0) {
					opts.append(",");
				}
				opts.append(setting.field);
				opts.append("=");
				opts.append(value);
			}
		}

		this.handshakeLevel = serverLevel;

		return opts.toString();
	}

	public enum Setting {
		AutoCommit("auto_commit", 1, 1),
		ReplySize("reply_size", 2, 100),
		SizeHeader("size_header", "sizeheader", 3, 0),
		// ColumnarProtocol("columnar_protocol", 4),
		TimeZone("time_zone", 5, 0),
		;

		private final int level;
		private final String field;
		private final String xcommand;
		private final int defaultValue;

		Setting(String field, int level, int defaultValue) {
			this(field, field, level, defaultValue);
		}

		Setting(String field, String xcommand, int level, int defaultValue) {
			this.field = field;
			this.xcommand = xcommand;
			this.level = level;
			this.defaultValue = defaultValue;
		}

		public boolean isSupported(int serverLevel) {
			return this.level < serverLevel;
		}

		public String getXCommand() {
			return xcommand;
		}

		public Integer getDefaultValue() {
			return defaultValue;
		}
	}
}
