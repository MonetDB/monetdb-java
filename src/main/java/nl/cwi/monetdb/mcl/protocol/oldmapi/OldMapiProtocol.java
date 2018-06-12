/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.connection.mapi.OldMapiSocket;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.responses.AbstractDataBlockResponse;
import nl.cwi.monetdb.mcl.responses.AutoCommitResponse;
import nl.cwi.monetdb.mcl.responses.ResultSetResponse;
import nl.cwi.monetdb.mcl.responses.UpdateResponse;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.CharBuffer;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * The JDBC abstract protocol implementation on a MAPI connection using the protocol version 9. The connection holds a
 * lineBuffer which will be reused during the whole connection for memory saving purposes. An additional tupleLineBuffer
 * is used to help parsing tuple lines from a BLOCK response.
 *
 * @author Pedro Ferreira
 */
public class OldMapiProtocol extends AbstractProtocol {

	/**
	 * The default size for the tuple lines' CharBuffer (it should be less than the OldMapiSocket BLOCK size).
	 */
	private static final int TUPLE_LINE_BUFFER_DEFAULT_SIZE = 1024;

	/** Format of a time string from the old MAPI connection */
	final SimpleDateFormat timeParser = new SimpleDateFormat("HH:mm:ss");

	/** Format of a timestamp string from the old MAPI connection */
	final SimpleDateFormat timestampParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**The current server response */
	private int currentServerResponseHeader = ServerResponses.UNKNOWN;

	/** The underlying MAPI socket connection */
	private final OldMapiSocket socket;

	/** The buffer used to parse server's responses */
	CharBuffer lineBuffer;

	/** A helper buffer used to parse tuple line responses */
	CharBuffer tupleLineBuffer;

	public OldMapiProtocol(OldMapiSocket socket) {
		this.socket = socket;
		this.lineBuffer = CharBuffer.wrap(new char[OldMapiSocket.FULL_BLOCK]);
		this.tupleLineBuffer = CharBuffer.wrap(new char[TUPLE_LINE_BUFFER_DEFAULT_SIZE]);
	}

	/**
	 * Gets the underlying socket.
	 *
	 * @return The underlying socket
	 */
	public OldMapiSocket getSocket() {
		return socket;
	}

	/**
	 * Gets the current server response, obtained through the fetchNextResponseData method.
	 *
	 * @return The integer representation of {@link ServerResponses}
	 */
	@Override
	public int getCurrentServerResponse() {
		return currentServerResponseHeader;
	}

	/**
	 * Reads up till the MonetDB prompt, indicating the server is ready for a new command.
	 *
	 * If there are errors present in the lines that are read, then they are put in one string and returned <b>after</b>
	 * the prompt has been found.
	 *
	 * @throws IOException if an IO exception occurs while talking to the server
	 */
	@Override
	public void waitUntilPrompt() throws IOException {
		while(this.currentServerResponseHeader != ServerResponses.PROMPT) {
			this.lineBuffer = this.socket.readLine(this.lineBuffer);
			if(this.lineBuffer.limit() == 0) {
				throw new IOException("Connection to server lost!");
			}
			this.currentServerResponseHeader = OldMapiServerResponseParser.parseOldMapiServerResponse(this);
			this.lineBuffer.position(0);
			if (this.currentServerResponseHeader == ServerResponses.ERROR) {
				this.lineBuffer.position(1);
			}
		}
	}

	/**
	 * Reads a line of text from the socket. A line is considered to be terminated by any one of a line feed ('\n').
	 *
	 * Warning: until the server properly prefixes all of its error messages with SQLSTATE codes, this method prefixes
	 * all errors it sees without sqlstate with the generic data exception code (22000).
	 *
	 * @throws IOException If an I/O error occurs
	 */
	@Override
	public void fetchNextResponseData() throws IOException {
		this.lineBuffer = this.socket.readLine(this.lineBuffer);
		if(this.lineBuffer.limit() == 0) {
			throw new IOException("Connection to server lost!");
		}
		this.currentServerResponseHeader = OldMapiServerResponseParser.parseOldMapiServerResponse(this);
		if (this.currentServerResponseHeader == ServerResponses.ERROR && !this.lineBuffer.toString()
				.matches("^[0-9A-Z]{5}!.+")) {
			int limit = this.lineBuffer.limit();
			CharBuffer newbuffer = CharBuffer.wrap(new char[this.lineBuffer.capacity() + 7]);
			newbuffer.put("!22000");
			newbuffer.put(this.lineBuffer.array(), 0, limit);
			newbuffer.limit(limit + 6);
			((Buffer)newbuffer).flip();
			this.lineBuffer = newbuffer;
		}
		this.lineBuffer.position(1);
	}

	/**
	 * Gets the next starter header of a server response.
	 *
	 * @return The integer representation of {@link StarterHeaders}
	 */
	@Override
	public int getNextStarterHeader() {
		return OldMapiStartOfHeaderParser.getNextStartHeaderOnOldMapi(this);
	}

	/**
	 * Gets the next ResultSet response from the server, belonging to a ResponseList.
	 *
	 * @param con The current MonetDB's JDBC connection
	 * @param list The Response List this result set will belong to
	 * @param seqnr The sequence number of this result set on the Response List
	 * @param maxrows A maxrows to set if so
	 * @return The ResultSet instance
	 * @throws ProtocolException If an error in the underlying connection happened.
	 */
	@Override
	public ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list, int seqnr,
													  int maxrows) throws ProtocolException {
		int id = OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this); //The order cannot be switched!!
		int tuplecount = OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this);
		int columncount = OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this);
		int rowcount = OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this);
		if (maxrows != 0 && tuplecount > maxrows) {
			tuplecount = maxrows;
		}
		return new ResultSetResponse(con, list, id, seqnr, rowcount, tuplecount, columncount);
	}

	/**
	 * Gets the next UpdateResponse response from the server.
	 *
	 * @return An UpdateResponse instance
	 * @throws ProtocolException If an error in the underlying connection happened.
	 */
	@Override
	public UpdateResponse getNextUpdateResponse() throws ProtocolException {
		int count = OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this); //The order cannot be switched!!
		int lastId = OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this);
		return new UpdateResponse(lastId, count);
	}

	/**
	 * Gets the next AutoCommitResponse response from the server.
	 *
	 * @return An AutoCommitResponse instance
	 * @throws ProtocolException If an error in the underlying connection happened.
	 */
	@Override
	public AutoCommitResponse getNextAutoCommitResponse() throws ProtocolException {
		boolean ac = this.lineBuffer.get() == 't';
		return new AutoCommitResponse(ac);
	}

	@Override
	public AbstractDataBlockResponse getAnEmptyDataBlockResponse(int rowcount, int columncount,
																 AbstractProtocol protocol, int[] JdbcSQLTypes) {
		return new OldMapiDataBlockResponse(rowcount, columncount, protocol, JdbcSQLTypes);
	}

	/**
	 * Gets the next DataBlockResponse response from the server, belonging to a ResultSetResponse
	 *
	 * @param rsresponses A map of ResultSetResponse, in which this Block will belong to one of them, by checking its id
	 *                    against the keys of the Map.
	 * @return The DataBlockResponse instance
	 * @throws ProtocolException If an error in the underlying connection happened.
	 */
	@Override
	public AbstractDataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses)
			throws ProtocolException {
		int id = OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this); //The order cannot be switched!!
		OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this); //column count
		int rowcount = OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this);
		int offset = OldMapiStartOfHeaderParser.getNextResponseDataAsInt(this);

		ResultSetResponse rs = rsresponses.get(id);
		if (rs == null) {
			return null;
		}
		return rs.addDataBlockResponse(offset, rowcount);
	}

	/**
	 * Gets the next Table Header for a ResultSetResponse. More than one of the parameter arrays can be filled at once.
	 *
	 * @param columnNames The column names array
	 * @param columnLengths The column lengths array
	 * @param types The columns SQL names array
	 * @param tableNames The columns schemas and names in format schema.table
	 * @return A TableResultHeaders integer representation, representing which of the fields was filled
	 * @throws ProtocolException If an error in the underlying connection happened.
	 */
	@Override
	public int getNextTableHeader(String[] columnNames, int[] columnLengths, String[] types, String[] tableNames)
			throws ProtocolException {
		return OldMapiTableHeaderParser.getNextTableHeader(this.lineBuffer, columnNames, columnLengths, types,
				tableNames);
	}

	/**
	 * Retrieves the next values in a DataBlockResponse from the underlying connection, starting at a specific line
	 * number.
	 *
	 * @param firstLineNumber The first line number in the response to retrieve
	 * @param typesMap The JDBC types mapping array for every column in the ResultSetResponse of the DataBlock
	 * @param values An array of columns to fill the values
	 * @return The number of lines parsed from the underlying connection
	 * @throws ProtocolException If an error in the underlying connection happened.
	 */
	int parseTupleLines(int firstLineNumber, int[] typesMap, Object[] values) throws ProtocolException {
		OldMapiTupleLineParser.oldMapiParseTupleLine(this, firstLineNumber, typesMap, values);
		return firstLineNumber;
	}

	/**
	 * Gets the remaining response line from the underlying connection as a Java String. This method is mostly used to
	 * retrieve error Strings, when they are detected while parsing a response line.
	 *
	 * @param startIndex The first index in the response line to retrieve the String
	 * @return The String representation of the line starting at the provided index
	 */
	@Override
	public String getRemainingStringLine(int startIndex) {
		if(this.lineBuffer.limit() > startIndex) {
			if(this.lineBuffer.array()[startIndex] == '!') {
				startIndex++;
			}
			return new String(this.lineBuffer.array(), startIndex, this.lineBuffer.limit() - startIndex);
		} else {
			return null;
		}
	}

	/**
	 * Writes a user query to the server, while providing the respective prefixes and suffixes depending on the current
	 * language and connection used.
	 *
	 * @param prefix The prefix to append at the beginning of the query string
	 * @param query The user query to submit to the server
	 * @param suffix The suffix to append at the end of the query string
	 * @throws IOException If an error in the underlying connection happened.
	 */
	@Override
	public void writeNextQuery(String prefix, String query, String suffix) throws IOException {
		this.socket.writeNextLine(prefix, query, suffix);
		// reset reader state, last line isn't valid any more now
		this.currentServerResponseHeader = ServerResponses.UNKNOWN;
	}
}
