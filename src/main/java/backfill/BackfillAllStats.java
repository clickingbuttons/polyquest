package backfill;

import java.util.ArrayList;
import java.util.List;

public class BackfillAllStats {
    double symbolTradeCount = 0;
    double tradeCount = 0;
    public List<Long> timesElapsed = new ArrayList<>();

    public void add(BackfillDayStats day) {
        symbolTradeCount += day.symbolsWithTrades.size();
        tradeCount += day.curNumTrades.get();

        timesElapsed.add(day.timeElapsed);
    }

    public String toString() {
        long totalMs = timesElapsed.stream().mapToLong(Long::longValue).sum();
        long averageMs = totalMs / timesElapsed.size();

        return String.format("Overall stats: %d days with %d trades in %ds (%ds/day): %d trades",
                timesElapsed.size(),
                symbolTradeCount,
                totalMs / 1000,
                averageMs / 1000,
                tradeCount);
    }
}
