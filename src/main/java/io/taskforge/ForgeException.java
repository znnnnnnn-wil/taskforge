package io.taskforge;

public class ForgeException extends RuntimeException {
    private final int status;

    public ForgeException(int status, String message) {
        super(message);
        this.status = status;
    }

    public int status() {
        return status;
    }
}
