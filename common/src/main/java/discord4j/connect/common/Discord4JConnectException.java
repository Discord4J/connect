package discord4j.connect.common;

public class Discord4JConnectException extends RuntimeException {

    public Discord4JConnectException() {
    }

    public Discord4JConnectException(String message) {
        super(message);
    }

    public Discord4JConnectException(String message, Throwable cause) {
        super(message, cause);
    }

    public Discord4JConnectException(Throwable cause) {
        super(cause);
    }
}
