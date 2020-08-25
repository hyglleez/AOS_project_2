public class DebugHelper {

    public enum Level {
        INFO,
        DEBUG
    }

    public static Level logLevel = Level.INFO;

    public static void log(Level level, String str) {
        if (logLevel.compareTo(level) >= 0) {
        	System.out.println(str);
        }
    }
}