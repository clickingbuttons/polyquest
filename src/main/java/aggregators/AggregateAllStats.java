package aggregators;

import java.util.ArrayList;
import java.util.List;

public class AggregateAllStats {
    double symbolAggregateCount = 0;
    public List<Long> timesElapsed = new ArrayList<>();

    public void add(AggregateDayStats day) {
        for (String aggLevel : day.numAggregates.keySet()) {
            symbolAggregateCount += day.numAggregates.get(aggLevel);
        }
        timesElapsed.add(day.timeElapsed);
    }

    public String toString() {
        long totalMs = timesElapsed.stream().mapToLong(Long::longValue).sum();
        long averageMs = totalMs / timesElapsed.size();

        return String.format("Overall stats: %d days with %d aggregates in %ds (%ds/day)",
                timesElapsed.size(),
                symbolAggregateCount,
                totalMs / 1000,
                averageMs / 1000);
    }
}
