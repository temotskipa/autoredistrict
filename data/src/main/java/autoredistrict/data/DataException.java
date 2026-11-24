package autoredistrict.data;

/** Input files or cached data can't be used; the message says what to do about it. */
public class DataException extends RuntimeException {

    public DataException(String message) {
        super(message);
    }

    public DataException(String message, Throwable cause) {
        super(message, cause);
    }
}
