package aggregators;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AggregateDayStats {
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    Calendar day;
    public Map<String, Double> numAggregates = new HashMap<>();

    public long timeElapsed;

    public AggregateDayStats(Calendar day) {
        this.day = day;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(sdf.format(day.getTime()));
        builder.append(" stats: aggregated ");
//        builder.append(numSymbols);
//        builder.append(" symbols with ");
        for (String key : numAggregates.keySet()) {
            builder.append(numAggregates.get(key));
            builder.append(" ");
            builder.append(key);
            builder.append(" ");
        }
        builder.append("in ");
        builder.append(timeElapsed / 1000);
        builder.append(" seconds");

        return builder.toString();
    }
}
