package org.qty.cloud.storage;

@SuppressWarnings("serial")
public class StorageOperationException extends RuntimeException {

    public StorageOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageOperationException(String message) {
        super(message);
    }

    public StorageOperationException(Throwable cause) {
        super(cause);
    }

}
