import java.util.ArrayList;
import java.util.List;

public class BackfillAllStats {
    int symbolTradeCount = 0;
    int symbol1mCount = 0;
    int symbol1dCount = 0;

    int tradeCount = 0;
    int agg1sCount = 0;
    int agg1mCount = 0;
    int agg5mCount = 0;
    int agg1hCount = 0;
    int agg1dCount = 0;


    public List<Long> timesElapsed = new ArrayList<>();

    public void add(BackfillDayStats day) {
        symbolTradeCount += day.symbolsWithTrades.size();
        tradeCount += day.curNumTrades.get();
        agg1sCount += day.curNum1s.get();
        symbol1mCount += day.symbolsWith1m.size();
        agg1mCount += day.curNum1m.get();
        agg5mCount += day.curNum5m.get();
        agg1hCount += day.curNum1h.get();
        symbol1dCount += day.symbolsWith1d.size();
        agg1dCount += day.curNum1d.get();

        timesElapsed.add(day.timeElapsed);
    }

    public String toString() {
        long totalMs = timesElapsed.stream().mapToLong(Long::longValue).sum();
        long averageMs = totalMs / timesElapsed.size();

        return String.format("Finished backfilling %d days in %ds (%ds/day): %d trades, " +
                        "%d 1m candles, %d 1d candles",
                timesElapsed.size(), totalMs / 1000, averageMs / 1000,
                tradeCount, agg1mCount, agg1dCount);
    }
}
