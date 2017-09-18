package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.jdbc.MonetBlob;
import nl.cwi.monetdb.jdbc.MonetClob;
import nl.cwi.monetdb.mcl.connection.helpers.TimestampHelper;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.responses.AbstractDataBlockResponse;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;

/**
 * DataBlockResponse for an Old MAPI connection.
 *
 * @author Fabin Groffen, Pedro Ferreira
 */
public class OldMapiDataBlockResponse extends AbstractDataBlockResponse {

	/** The array to keep the data in */
	private Object[] data;
	/** The counter which keeps the current position in the lines array */
	private int pos;
	/** The last parsed nanos values for timestamps */
	private int lastNanos;

	OldMapiDataBlockResponse(int rowcount, int columncount, AbstractProtocol protocol, int[] JdbcSQLTypes) {
		super(rowcount, protocol, JdbcSQLTypes);
		this.pos = -1;
		this.data = new Object[columncount];
	}

	@Override
	public void addLines(AbstractProtocol protocol) throws ProtocolException {
		int csrh = protocol.getCurrentServerResponse();
		if (csrh != ServerResponses.RESULT) {
			throw new ProtocolException("protocol violation: unexpected line in data block: " +
					protocol.getRemainingStringLine(0));
		}
		if(this.pos == -1) { //if it's the first line, initialize the matrix
			int numberOfColumns = this.data.length;
			for (int i = 0 ; i < numberOfColumns ; i++) {
				switch (this.jdbcSQLTypes[i]) {
					case Types.INTEGER:
						this.data[i] = new int[this.rowcount];
						break;
					case Types.BOOLEAN:
					case Types.TINYINT:
						this.data[i] = new byte[this.rowcount];
						break;
					case Types.SMALLINT:
						this.data[i] = new short[this.rowcount];
						break;
					case Types.REAL:
						this.data[i] = new float[this.rowcount];
						break;
					case Types.DOUBLE:
						this.data[i] = new double[this.rowcount];
						break;
					case Types.BIGINT:
						this.data[i] = new long[this.rowcount];
						break;
					case Types.DATE:
					case Types.TIME:
					case 2013: //Types.TIME_WITH_TIMEZONE:
						this.data[i] = new Calendar[this.rowcount];
						break;
					case Types.TIMESTAMP:
					case 2014: //Types.TIMESTAMP_WITH_TIMEZONE:
						this.data[i] = new TimestampHelper[this.rowcount];
						break;
					case Types.NUMERIC:
					case Types.DECIMAL:
						this.data[i] = new BigDecimal[this.rowcount];
						break;
					case Types.BLOB:
						this.data[i] = new MonetBlob[this.rowcount];
						break;
					case Types.CLOB:
						this.data[i] = new MonetClob[this.rowcount];
						break;
					case Types.LONGVARBINARY:
						this.data[i] = new byte[this.rowcount][];
						break;
					default: //CHAR, VARCHAR, OTHER
						this.data[i] = new String[this.rowcount];
				}
			}
		}
		// add to the backing array
		int nextPos = this.pos + 1;
		this.pos = ((OldMapiProtocol)this.protocol).parseTupleLines(nextPos, this.jdbcSQLTypes, this.data);
	}

	/**
	 * Returns whether this Response expects more lines to be added to it.
	 *
	 * @return true if a next line should be added, false otherwise
	 */
	@Override
	public boolean wantsMore() {
		// remember: pos is the value already stored
		return (this.pos + 1) < this.rowcount;
	}

	/**
	 * Instructs the Response implementation to close and do the necessary clean up procedures.
	 */
	@Override
	public void close() {
		// feed all rows to the garbage collector
		int numberOfColumns = this.data.length;
		for (int i = 0; i < numberOfColumns; i++) {
			data[i] = null;
		}
		data = null;
	}

	/**
	 * Checks if a value in the current row is null.
	 *
	 * @param column The column index starting from 0
	 * @return If the value is null or not.
	 */
	private boolean checkValueIsNull(int column) {
		switch (this.jdbcSQLTypes[column]) {
			case Types.BOOLEAN:
			case Types.TINYINT:
				this.lastReadWasNull = ((byte[]) this.data[column])[this.blockLine] == Byte.MIN_VALUE;
				break;
			case Types.SMALLINT:
				this.lastReadWasNull = ((short[]) this.data[column])[this.blockLine] == Short.MIN_VALUE;
				break;
			case Types.INTEGER:
				this.lastReadWasNull = ((int[]) this.data[column])[this.blockLine] == Integer.MIN_VALUE;
				break;
			case Types.BIGINT:
				this.lastReadWasNull = ((long[]) this.data[column])[this.blockLine] == Long.MIN_VALUE;
				break;
			case Types.REAL:
				this.lastReadWasNull = ((float[]) this.data[column])[this.blockLine] == Float.MIN_VALUE;
				break;
			case Types.DOUBLE:
				this.lastReadWasNull = ((double[]) this.data[column])[this.blockLine] == Double.MIN_VALUE;
				break;
			default:
				this.lastReadWasNull = ((Object[]) this.data[column])[this.blockLine] == null;
		}
		return this.lastReadWasNull;
	}

	@Override
	public boolean getBooleanValue(int column) {
		return !this.checkValueIsNull(column) && ((byte[]) this.data[column])[this.blockLine] == 1;
	}

	@Override
	public byte getByteValue(int column) {
		if(this.checkValueIsNull(column)) {
			return 0;
		}
		return ((byte[]) this.data[column])[this.blockLine];
	}

	@Override
	public short getShortValue(int column) {
		if(this.checkValueIsNull(column)) {
			return 0;
		}
		return ((short[]) this.data[column])[this.blockLine];
	}

	@Override
	public int getIntValue(int column) {
		if(this.checkValueIsNull(column)) {
			return 0;
		}
		return ((int[]) this.data[column])[this.blockLine];
	}

	@Override
	public long getLongValue(int column) {
		if(this.checkValueIsNull(column)) {
			return 0;
		}
		return ((long[]) this.data[column])[this.blockLine];
	}

	@Override
	public float getFloatValue(int column) {
		if(this.checkValueIsNull(column)) {
			return 0.0f;
		}
		return ((float[]) this.data[column])[this.blockLine];
	}

	@Override
	public double getDoubleValue(int column) {
		if(this.checkValueIsNull(column)) {
			return 0.0f;
		}
		return ((double[]) this.data[column])[this.blockLine];
	}

	@Override
	public Object getObjectValue(int column) {
		if(this.checkValueIsNull(column)) {
			return null;
		}
		return ((Object[]) this.data[column])[this.blockLine];
	}

	@Override
	public String getValueAsString(int column) {
		switch (this.jdbcSQLTypes[column]) {
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.OTHER:
				return ((String[]) this.data[column])[this.blockLine];
			case Types.LONGVARBINARY:
				return Arrays.toString(((byte[][]) this.data[column])[this.blockLine]);
			case Types.BOOLEAN:
				return ((byte[]) this.data[column])[this.blockLine] == 1 ? "true" : "false";
			case Types.TINYINT:
				return Byte.toString(((byte[]) this.data[column])[this.blockLine]);
			case Types.SMALLINT:
				return Short.toString(((short[]) this.data[column])[this.blockLine]);
			case Types.INTEGER:
				return Integer.toString(((int[]) this.data[column])[this.blockLine]);
			case Types.BIGINT:
				return Long.toString(((long[]) this.data[column])[this.blockLine]);
			case Types.REAL:
				return Float.toString(((float[]) this.data[column])[this.blockLine]);
			case Types.DOUBLE:
				return Double.toString(((double[]) this.data[column])[this.blockLine]);
			case Types.DATE:
				Date aux1 = new Date(((Calendar[]) this.data[column])[this.blockLine].getTimeInMillis());
				return protocol.getMonetDate().format(aux1);
			case Types.TIME:
				Time aux2 = new Time(((Calendar[]) this.data[column])[this.blockLine].getTimeInMillis());
				return protocol.getMonetTimePrinter().format(aux2);
			case 2013: //Types.TIME_WITH_TIMEZONE:
				Time aux3 = new Time(((Calendar[]) this.data[column])[this.blockLine].getTimeInMillis());
				return protocol.getMonetTimeTzPrinter().format(aux3);
			case Types.TIMESTAMP:
				TimestampHelper thel = ((TimestampHelper[]) this.data[column])[this.blockLine];
				Timestamp aux4 = thel.getTimestamp();
				this.lastNanos = thel.getNanoseconds();
				return protocol.getMonetTimestampPrinter().format(aux4);
			case 2014: //Types.TIMESTAMP_WITH_TIMEZONE:
				TimestampHelper thelper = ((TimestampHelper[]) this.data[column])[this.blockLine];
				Timestamp aux5 = thelper.getTimestamp();
				this.lastNanos = thelper.getNanoseconds();
				return protocol.getMonetTimestampTzPrinter().format(aux5);
			default: //BLOB, CLOB, BigDecimal
				return ((Object[]) this.data[column])[this.blockLine].toString();
		}
	}

	@Override
	public Object getValueAsObject(int column) {
		switch (this.jdbcSQLTypes[column]) {
			case Types.BOOLEAN:
				return ((byte[]) this.data[column])[this.blockLine] == 1;
			case Types.TINYINT:
				return ((byte[]) this.data[column])[this.blockLine];
			case Types.SMALLINT:
				return ((short[]) this.data[column])[this.blockLine];
			case Types.INTEGER:
				return ((int[]) this.data[column])[this.blockLine];
			case Types.BIGINT:
				return ((long[]) this.data[column])[this.blockLine];
			case Types.REAL:
				return ((float[]) this.data[column])[this.blockLine];
			case Types.DOUBLE:
				return ((double[]) this.data[column])[this.blockLine];
			case Types.TIMESTAMP:
			case 2014: //Types.TIMESTAMP_WITH_TIMEZONE:
				TimestampHelper thelper = ((TimestampHelper[]) this.data[column])[this.blockLine];
				this.lastNanos = thelper.getNanoseconds();
				return thelper.getCalendar();
			default:
				return ((Object[]) this.data[column])[this.blockLine];
		}
	}

	@Override
	public int getLastNanos() {
		return this.lastNanos;
	}
}
