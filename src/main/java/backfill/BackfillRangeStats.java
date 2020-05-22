package backfill;

import polygon.models.Dividend;
import polygon.models.OHLCV;
import polygon.models.Split;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BackfillRangeStats {
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    String type;
    Calendar from;
    Calendar to;
    long startTime;
    long flushTime;
    long downloadTime;
    AtomicInteger numRows = new AtomicInteger(0);
    Map<String, List<String>> uniqueSymbols = new ConcurrentHashMap<>();

    public BackfillRangeStats(String type, Calendar from, Calendar to) {
        this.type = type;
        this.from = (Calendar) from.clone();
        this.to = (Calendar) to.clone();
        this.startTime = System.currentTimeMillis();
    }

    public String toString() {
        return String.format("Backfilled %s from %s to %s: %d rows in %d+%d seconds",
                type,
                sdf.format(from.getTime()),
                sdf.format(to.getTime()),
                numRows.longValue(),
                (downloadTime - startTime) / 1000,
                (flushTime - downloadTime) / 1000);
    }

    public void completeAggs(List<OHLCV> aggs) {
        for (OHLCV agg : aggs) {
            String date = Instant.ofEpochMilli(agg.timeMicros / 1000).toString().substring(0, 10);
            uniqueSymbols.putIfAbsent(agg.ticker, new ArrayList<>());
            uniqueSymbols.get(agg.ticker).add(date);
        }
        numRows.addAndGet(aggs.size());
    }

    public void completeDividends(List<Dividend> dividend) {
        for (Dividend div : dividend) {
            uniqueSymbols.putIfAbsent(div.ticker, new ArrayList<>());
            uniqueSymbols.get(div.ticker).add(div.exDate);
        }
        numRows.addAndGet(dividend.size());
    }

    public void completeSplits(List<Split> splits) {
        for (Split s : splits) {
            uniqueSymbols.putIfAbsent(s.ticker, new ArrayList<>());
            uniqueSymbols.get(s.ticker).add(s.exDate);
        }
        numRows.addAndGet(splits.size());
    }

    public void completeDownload() {
        this.downloadTime = System.currentTimeMillis();
    }

    public void completeFlush() {
        this.flushTime = System.currentTimeMillis();
    }
}
