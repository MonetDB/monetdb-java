package nl.cwi.monetdb.mcl.connection.helpers;

import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Due to the poor design of the old Java date and time API, when retrieving timestamps from the MAPI connection, this
 * class is used to store the calendar with timezone information and the nanoseconds information to generate a
 * {@link Timestamp} instance, as there is no mapping in Java Classes to store both information.
 *
 * @author Pedro Ferreira
 */
public class TimestampHelper {

    /** The calendar instance */
    private Calendar calendar;

    /** The nanoseconds information */
    private int nanoseconds;

    TimestampHelper(Calendar calendar, int nanoseconds) {
        this.calendar = calendar;
        this.nanoseconds = nanoseconds;
    }

    /**
     * Gets the Calendar instance.
     *
     * @return The Calendar instance
     */
    public Calendar getCalendar() {
        return calendar;
    }

    /**
     * Sets the Calendar instance.
     *
     * @param calendar The Calendar instance
     */
    public void setCalendar(Calendar calendar) {
        this.calendar = calendar;
    }

    /**
     * Gets the nanoseconds information.
     *
     * @return The nanoseconds information
     */
    public int getNanoseconds() {
        return nanoseconds;
    }

    /**
     * Sets the nanoseconds information.
     *
     * @param nanoseconds The nanoseconds information
     */
    public void setNanoseconds(int nanoseconds) {
        this.nanoseconds = nanoseconds;
    }

    /**
     * Generates a {@link Timestamp} instance from the provided information.
     *
     * @return The generated {@link Timestamp} instance
     */
    public Timestamp getTimestamp() {
        Timestamp res = new Timestamp(calendar.getTimeInMillis());
        res.setNanos(nanoseconds);
        return res;
    }
}
