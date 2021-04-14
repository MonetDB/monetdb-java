/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

package org.monetdb.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public final class CmdLineOpts {
	/** the arguments we handle */
	private HashMap<String, OptionContainer> opts = new HashMap<String, OptionContainer>();
	/** the options themself */
	private ArrayList<OptionContainer> options = new ArrayList<OptionContainer>();

	/** no arguments */
	public static final int CAR_ZERO	= 0;
	/** always one argument */
	public static final int CAR_ONE	= 1;
	/** zero or one argument */
	public static final int CAR_ZERO_ONE	= 2;
	/** zero or many arguments */
	public static final int CAR_ZERO_MANY	= 3;
	/** one or many arguments */
	public static final int CAR_ONE_MANY	= 4;

	public CmdLineOpts() {
	}

	public void addOption(
			final String shorta,
			final String longa,
			final int type,
			final String defaulta,
			final String descriptiona)
		throws OptionsException {
		final OptionContainer oc =
			new OptionContainer(
				shorta,
				longa,
				type,
				defaulta,
				descriptiona
			);
		if (shorta != null)
			opts.put(shorta, oc);
		if (longa != null)
			opts.put(longa, oc);
	}

	public void removeOption(final String name) {
		final OptionContainer oc = opts.get(name);
		if (oc != null) {
			opts.remove(oc.shorta);
			opts.remove(oc.longa);
		}
	}

	public OptionContainer getOption(final String key) throws OptionsException {
		final OptionContainer ret = opts.get(key);
		if (ret == null)
			throw new OptionsException("No such option: " + key);

		return ret;
	}

	public void processFile(final java.io.File file) throws OptionsException {
		// the file is there, parse it and set its settings
		final Properties prop = new Properties();
		try {
			final java.io.FileInputStream in = new java.io.FileInputStream(file);
			try {
				prop.load(in);
			} finally {
				in.close();
			}

			String key;
			OptionContainer option = null;
			for (java.util.Enumeration<?> e = prop.propertyNames(); e.hasMoreElements(); ) {
				key = (String) e.nextElement();
				option = opts.get(key);
				if (option == null)
					throw new OptionsException("Unknown option: " + key);
				option.resetArguments();
				option.addArgument(prop.getProperty(key));
			}
		} catch (java.io.IOException e) {
			// well... then forget about it
		}
	}

	public void processArgs(final String args[]) throws OptionsException {
		// parse and set the command line arguments
		OptionContainer option = null;
		int quant = -1;
		int qcount = 0;
		boolean moreData = false;
		for (int i = 0; i < args.length; i++) {
			if (option == null) {
				if (args[i].charAt(0) != '-')
					throw new OptionsException("Unexpected value: " + args[i]);

				// see what kind of argument we have
				if (args[i].length() == 1)
					throw new OptionsException("Illegal argument: '-'");

				if (args[i].charAt(1) == '-') {
					// we have a long argument
					// since we don't accept inline values we can take
					// everything left in the string as argument, unless
					// there is a = in there...
					final String tmp = args[i].substring(2);
					final int pos = tmp.indexOf('=');
					if (pos == -1) {
						option = opts.get(tmp);
						moreData = false;
					} else {
						option = opts.get(tmp.substring(0, pos));
						// modify the option a bit so the code below
						// handles the moreData correctly
						args[i] = "-?" + tmp.substring(pos + 1);
						moreData = true;
					}
				} else if (args[i].charAt(1) == 'X') {
					// extra argument, same as long argument
					final String tmp = args[i].substring(1);
					final int pos = tmp.indexOf('=');
					if (pos == -1) {
						option = opts.get(tmp);
						moreData = false;
					} else {
						option = opts.get(tmp.substring(0, pos));
						// modify the option a bit so the code below
						// handles the moreData correctly
						args[i] = "-?" + tmp.substring(pos + 1);
						moreData = true;
					}
				} else {
					// single char argument
					option = opts.get("" + args[i].charAt(1));
					// is there more data left in the argument?
					moreData = args[i].length() > 2 ? true : false;
				}

				if (option != null) {
					// make sure we overwrite previously set arguments
					option.resetArguments();
					final int card = option.getCardinality();
					if (card == CAR_ONE) {
						if (moreData) {
							option.addArgument(args[i].substring(2));
							option = null;
						} else {
							quant = 1;
						}
					} else if (card == CAR_ZERO_ONE) {
						option.setPresent();
						qcount = 1;
						quant = 2;
						if (moreData) {
							option.addArgument(args[i].substring(2));
							option = null;
						}
					} else if (card == CAR_ZERO_MANY) {
						option.setPresent();
						qcount = 1;
						quant = -1;
						if (moreData) {
							option.addArgument(args[i].substring(2));
							qcount++;
						}
					} else if (card == CAR_ZERO) {
						option.setPresent();
						option = null;
					}
				} else {
					throw new OptionsException("Unknown argument: " + args[i]);
				}
			} else {
				// store the `value'
				option.addArgument(args[i]);
				if (++qcount == quant) {
					quant = 0;
					qcount = 0;
					option = null;
				}
			}
		}
	}

	/**
	 * Returns a help message for all options loaded.
	 *
	 * @return the help message
	 */
	public String produceHelpMessage() {
		// first calculate how much space is necessary for the options
		int maxlen = 0;
		for (OptionContainer oc : options) {
			final String shorta = oc.getShort();
			final String longa = oc.getLong();
			int len = 0;
			if (shorta != null)
				len += shorta.length() + 1 + 1;
			if (longa != null)
				len += longa.length() + 1 + (longa.charAt(0) == 'X' ? 0 : 1) + 1;
			// yes, we don't care about branch mispredictions here ;)
			if (maxlen < len)
				maxlen = len;
		}

		// get the individual strings
		final StringBuilder ret = new StringBuilder();
		for (OptionContainer oc : options) {
			ret.append(produceHelpMessage(oc, maxlen));
		}

		return ret.toString();
	}

	/**
	 * Returns the help message for the given OptionContainer.  The
	 * command line flags will be padded to optwidth spaces to allow for
	 * aligning.  The description of the flags will be wrapped at 80
	 * characters.
	 *
	 * @param oc the OptionContainer
	 * @param indentwidth padding width for the command line flags
	 * @return the help message for the option
	 */
	public String produceHelpMessage(final OptionContainer oc, int indentwidth) {
		final String desc = oc.getDescription();
		if (desc == null)
			return "";

		final String shorta = oc.getShort();
		final String longa = oc.getLong();
		int optwidth = 0;
		if (shorta != null)
			optwidth += shorta.length() + 1 + 1;
		if (longa != null)
			optwidth += longa.length() + 1 + (longa.charAt(0) == 'X' ? 0 : 1) + 1;
		final int descwidth = 80 - indentwidth;

		// default to with of command line flags if no width given
		if (indentwidth <= 0)
			indentwidth = optwidth;

		final StringBuilder ret = new StringBuilder();

		// add the command line flags
		if (shorta != null)
			ret.append('-').append(shorta).append(' ');
		if (longa != null) {
			ret.append('-');
			if (longa.charAt(0) != 'X')
				ret.append('-');
			ret.append(longa).append(' ');
		}

		for (int i = optwidth; i < indentwidth; i++)
			ret.append(' ');
		// add the description, wrap around words
		int pos = 0, lastpos = 0;
		while (pos < desc.length()) {
			pos += descwidth;
			if (lastpos != 0) {
				for (int i = 0; i < indentwidth; i++)
					ret.append(' ');
			}
			if (pos >= desc.length()) {
				ret.append(desc.substring(lastpos)).append('\n');
				break;
			}
			int space;
			for (space = pos; desc.charAt(space) != ' '; space--) {
				if (space == 0) {
					space = pos;
					break;
				}
			}
			pos = space;
			ret.append(desc.substring(lastpos, pos)).append('\n');
			while (desc.charAt(pos) == ' ')
				pos++;
			lastpos = pos;
		}

		return ret.toString();
	}

	public final class OptionContainer {
		private final int cardinality;
		private final String shorta;
		private final String longa;
		private final ArrayList<String> values = new ArrayList<String>();
		private final String name;
		private final String defaulta;
		private final String descriptiona;
		private boolean present;

		public OptionContainer(
				final String shorta,
				final String longa,
				final int cardinality,
				final String defaulta,
				final String descriptiona)
			throws IllegalArgumentException
		{
			this.cardinality = cardinality;
			this.shorta = shorta;
			this.longa = longa;
			this.defaulta = defaulta;
			this.descriptiona = descriptiona;
			this.present = false;

			if (cardinality != CAR_ZERO &&
			    cardinality != CAR_ONE &&
			    cardinality != CAR_ZERO_ONE &&
			    cardinality != CAR_ZERO_MANY &&
			    cardinality != CAR_ONE_MANY)
				throw new IllegalArgumentException("unknown cardinality");
			if (shorta != null && shorta.length() != 1)
				throw new IllegalArgumentException("short option should consist of exactly one character");
			if (shorta == null && longa == null)
				throw new IllegalArgumentException("either a short or long argument should be given");
			if ((cardinality == CAR_ZERO ||
			     cardinality == CAR_ZERO_ONE ||
			     cardinality == CAR_ZERO_MANY) &&
			    defaulta != null) {
				throw new IllegalArgumentException("cannot specify a default for a (possible) zero argument option");
			}

			name = (longa != null) ? longa : shorta;
			options.add(this);
		}

		public final void resetArguments() {
			values.clear();
		}

		public void addArgument(final String val) throws OptionsException {
			if (cardinality == CAR_ZERO) {
				throw new OptionsException("option " + name + " does not allow arguments");
			}
			if ((cardinality == CAR_ONE || cardinality == CAR_ZERO_ONE) && values.size() >= 1) {
				throw new OptionsException("option " + name + " does at max allow only one argument");
			}
			// we can add it
			values.add(val);
			setPresent();
		}

		public final void setPresent() {
			present = true;
		}

		public final boolean isPresent() {
			return present;
		}

		public final int getCardinality() {
			return cardinality;
		}

		public final int getArgumentCount() {
			return values.size();
		}

		public final String getArgument() {
			final String ret = getArgument(1);
			if (ret == null)
				return defaulta;
			return ret;
		}

		public final String getArgument(final int index) {
			final String[] args = getArguments();
			if (index < 1 || index > args.length)
				return null;
			return args[index - 1];
		}

		public final String[] getArguments() {
			return values.toArray(new String[values.size()]);
		}

		public final String getShort() {
			return shorta;
		}

		public final String getLong() {
			return longa;
		}

		public final String getDescription() {
			return descriptiona;
		}
	}
}
