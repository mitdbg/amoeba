package core.utils;

public class ThreadUtils {
    public static void sleep(long intervalInMillis) {
        try {
            Thread.sleep(intervalInMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to sleep the thread: "
                    + e.getMessage());
        }
    }
}
