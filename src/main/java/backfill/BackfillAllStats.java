package backfill;

import java.util.ArrayList;
import java.util.List;

public class BackfillAllStats {
    double symbolTradeCount = 0;
    double tradeCount = 0;
    public List<Long> timesElapsed = new ArrayList<>();

    public void add(BackfillRangeStats day) {
        symbolTradeCount += day.symbolsWithData.size();
        tradeCount += day.curNumRows.get();

        timesElapsed.add(day.timeElapsed);
    }

    public String toString() {
        long totalMs = timesElapsed.stream().mapToLong(Long::longValue).sum();
        long averageMs = totalMs / timesElapsed.size();

        return String.format("Overall stats: %d days with %f trades in %ds (%ds/day): %f trades",
                timesElapsed.size(),
                symbolTradeCount,
                totalMs / 1000,
                averageMs / 1000,
                tradeCount);
    }
}
