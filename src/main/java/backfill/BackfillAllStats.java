package backfill;

import java.util.ArrayList;
import java.util.List;

public class BackfillAllStats {
    public List<BackfillRangeStats> rangeStats = new ArrayList<>();

    public void add(BackfillRangeStats day) {
        rangeStats.add(day);
    }

    public String toString() {
        long totalMs = rangeStats.stream().mapToLong(stat -> stat.runTime).sum();
        long rowCount = rangeStats.stream().mapToLong(stat -> stat.numRows).sum();

        return String.format("Overall stats: %d ranges with %d symbols with %d rows in %ds (%ds/day)",
                rangeStats.size(),
                rangeStats.stream().flatMap(stat -> stat.uniqueSymbols.stream()).distinct().count(),
                rowCount,
                totalMs / 1000,
                totalMs / rangeStats.size()
        );
    }
}
