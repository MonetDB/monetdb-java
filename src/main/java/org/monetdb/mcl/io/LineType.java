package org.monetdb.mcl.io;

public enum LineType {
	/** "there is currently no line", or the the type is unknown is represented by UNKNOWN */
	UNKNOWN(null),

	/** a line starting with ! indicates ERROR */
	ERROR(new byte[] { '!' }),

	/** a line starting with % indicates HEADER */
	HEADER(new byte[] { '%' }),

	/** a line starting with [ indicates RESULT */
	RESULT(new byte[] { '[' }),

	/** a line which matches the pattern of prompt1 is a PROMPT */
	PROMPT(new byte[] { 1, 1 }),

	/** a line which matches the pattern of prompt2 is a MORE */
	MORE(new byte[] { 1, 2 }),

	/** a line which matches the pattern of prompt3 is a FILETRANSFER */
	FILETRANSFER(new byte[] { 1, 3 }),

	/** a line starting with &amp; indicates the start of a header block */
	SOHEADER(new byte[] { '&' }),

	/** a line starting with ^ indicates REDIRECT */
	REDIRECT(new byte[] { '^' }),

	/** a line starting with # indicates INFO */
	INFO(new byte[] { '#' });

	private final byte[] bytes;

	LineType(byte[] bytes) {
		this.bytes = bytes;
	}

	public final byte[] bytes() {
		return this.bytes;
	}

	public static final LineType classify(String line) {
		if (line == null) {
			return UNKNOWN;
		}
		if (line.length() > 1) {
			return classify(line.charAt(0), line.charAt(1));
		} else if (line.length() == 1) {
			return classify(line.charAt(0), 0);
		} else {
			return UNKNOWN;
		}
	}

	public static final LineType classify(byte[] line) {
		if (line == null) {
			return UNKNOWN;
		}
		if (line.length > 1) {
			return classify(line[0], line[1]);
		} else if (line.length == 1) {
			return classify(line[0], 0);
		} else {
			return UNKNOWN;
		}
	}

	private static final LineType classify(int ch0, int ch1) {
		switch (ch0) {
			case '!':
				return ERROR;
			case '%':
				return HEADER;
			case '[':
				return RESULT;
			case '&':
				return SOHEADER;
			case '^':
				return REDIRECT;
			case '#':
				return INFO;
			case 1:
				// prompts, see below
				break;
			default:
				return UNKNOWN;
		}

		switch (ch1) {
			case 1:
				return PROMPT;
			case 2:
				return MORE;
			case 3:
				return FILETRANSFER;
			default:
				return UNKNOWN;
		}
	}
}
