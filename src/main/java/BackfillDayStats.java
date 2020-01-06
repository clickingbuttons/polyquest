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
    public AtomicInteger curNum1s = new AtomicInteger(0);
    public AtomicInteger curNum1m = new AtomicInteger(0);
    public AtomicInteger curNum5m = new AtomicInteger(0);
    public AtomicInteger curNum1h = new AtomicInteger(0);
    public AtomicInteger curNum1d = new AtomicInteger(0);

    public List<String> symbolsWithTrades = new ArrayList<>();
    public List<String> symbolsWith1m = new ArrayList<>();
    public List<String> symbolsWith1d = new ArrayList<>();
    public long timeElapsed;

    public BackfillDayStats(Calendar day, int numSymbols) {
        this.day = day;
        this.numSymbols = numSymbols;
    }

    public String toString() {
        return String.format("%s stats: %d symbols with %d trades," +
                        " %d symbols with %d 1m candles, %d symbols with %d 1d candles in %d seconds",
                sdf.format(day.getTime()),
                symbolsWithTrades.size(), curNumTrades.get(),
                symbolsWith1m.size(), curNum1m.get(),
                symbolsWith1d.size(), curNum1d.get(),
                timeElapsed / 1000);
    }
}
