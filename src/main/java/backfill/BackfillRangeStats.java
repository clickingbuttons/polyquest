package backfill;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BackfillRangeStats {
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    String type;
    Calendar from;
    Calendar to;
    public int numSymbols;
    public AtomicInteger curNumSymbols = new AtomicInteger(0);
    public AtomicInteger curNumRows = new AtomicInteger(0);

    public List<String> symbolsWithData = new ArrayList<>();
    public long timeElapsed;

    public BackfillRangeStats(String type, Calendar from, Calendar to, int numSymbols) {
        this.type = type;
        this.from = (Calendar) from.clone();
        this.to = (Calendar) to.clone();
        this.numSymbols = numSymbols;
    }

    public String toString() {
        return String.format("%s to %s %s stats: %d/%d symbols with %d rows in %d seconds",
                sdf.format(from.getTime()),
                sdf.format(to.getTime()),
                type,
                symbolsWithData.size(),
                numSymbols,
                curNumRows.get(),
                timeElapsed / 1000);
    }
}
