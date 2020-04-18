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
    List<String> uniqueSymbols;
    long flushTime;
    long runTime;
    long numRows;

    public BackfillRangeStats(String type, Calendar from, Calendar to, long runTime, long flushTime, List<String> uniqueSymbols, long numRows) {
        this.type = type;
        this.from = (Calendar) from.clone();
        this.to = (Calendar) to.clone();
        this.uniqueSymbols = uniqueSymbols;
        this.numRows = numRows;
        this.runTime = runTime;
        this.flushTime = flushTime;
    }

    public String toString() {
        return String.format("Backfilled %s from %s to %s: %d symbols with %d rows in %d seconds",
                type,
                sdf.format(from.getTime()),
                sdf.format(to.getTime()),
                uniqueSymbols.size(),
                numRows,
                runTime / 1000);
    }
}
