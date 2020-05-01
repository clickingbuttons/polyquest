package backfill;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class BackfillAllStats {
    public List<BackfillRangeStats> rangeStats = new ArrayList<>();
    Map<String, List<String>> uniqueSymbols = new ConcurrentHashMap<>();

    public void add(BackfillRangeStats day) {
        rangeStats.add(day);
        for (Map.Entry<String, List<String>> entry : day.uniqueSymbols.entrySet()) {
            String ticker = entry.getKey();
            List<String> days = entry.getValue();
            uniqueSymbols.putIfAbsent(ticker, new ArrayList<>());
            uniqueSymbols.get(ticker).addAll(days);
        }
    }

    public String toString() {
        long totalMs = rangeStats.stream().mapToLong(stat -> stat.flushTime - stat.startTime).sum();
        long rowCount = rangeStats.stream().mapToLong(stat -> stat.numRows.longValue()).sum();

        return String.format("Overall stats: %d ranges with %d symbols with %d rows in %ds",
                rangeStats.size(),
                uniqueSymbols.size(),
                rowCount,
                totalMs / 1000
        );
    }

    public void writeSymbols(String filename) {
        try (PrintWriter out = new PrintWriter(filename)) {
            out.println("ticker");
            uniqueSymbols.keySet().stream().sorted().forEach(out::println);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static String getLine(String ticker, List<String> dates) {
        StringBuilder sb = new StringBuilder();
        sb.append("  {\"ticker\":\"").append(ticker).append('"');
        dates.removeIf(Objects::isNull);
        java.util.Collections.sort(dates);
        sb.append(",\"days\":[");
        for (int i = 0; i < dates.size(); i++) {
            sb.append('"').append(dates.get(i)).append('"');
            if (i != dates.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    public void writeJSON(String filename) {
        try (PrintWriter out = new PrintWriter(filename)) {
            out.println("[");
            List<String> tickers = new ArrayList<>(uniqueSymbols.keySet());
            java.util.Collections.sort(tickers);
            for (int i = 0; i < tickers.size(); i++) {
                String ticker = tickers.get(i);
                out.print(getLine(ticker, uniqueSymbols.get(ticker)));
                if (i != tickers.size() - 1) {
                    out.print(",");
                }
                out.println();
            }
            out.println("]");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
