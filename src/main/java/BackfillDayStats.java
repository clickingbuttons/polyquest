import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BackfillDayStats {
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    Calendar day;
    public int numSymbols;
    public AtomicInteger curNumSymbols = new AtomicInteger(0);
    public AtomicInteger curNumTrades = new AtomicInteger(0);

    public List<String> symbolsWithTrades = new ArrayList<>();
    public long timeElapsed;

    public BackfillDayStats(Calendar day, int numSymbols) {
        this.day = day;
        this.numSymbols = numSymbols;
    }

    public String toString() {
        return String.format("%s stats: %d/%d symbols with %d trades in %d seconds",
                sdf.format(day.getTime()),
                symbolsWithTrades.size(),
                numSymbols,
                curNumTrades.get(),
                timeElapsed / 1000);
    }
}
