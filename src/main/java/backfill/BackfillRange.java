package backfill;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.PolygonClient;
import polygon.models.OHLCV;
import polygon.models.Ticker;
import questdb.QuestDBWriter;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BackfillRange {
    final static Logger logger = LogManager.getLogger(BackfillRange.class);
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static AtomicInteger agg1dCounter = new AtomicInteger(0);

    public enum BackfillMethod {
        grouped,
        aggs
    }

    @SuppressWarnings("unchecked")
    static BackfillRangeStats backfillRangeGrouped(String tableName, Calendar from, Calendar to) {
        BackfillRangeStats stats = new BackfillRangeStats(tableName, from, to);

        List<CompletableFuture<List<OHLCV>>> grouped = Stream
                .iterate(from, day -> {
                    Calendar nextDay = (Calendar) day.clone();
                    nextDay.add(Calendar.DATE, 1);
                    agg1dCounter.set(0);
                    return nextDay;
                })
                .limit(ChronoUnit.DAYS.between(from.toInstant(), to.toInstant()))
                .filter(MarketCalendar::isMarketOpen)
                .map(day -> CompletableFuture.supplyAsync(() -> {
                    List<OHLCV> aggs = PolygonClient.getAgg1d(day);
                    stats.completeRows(aggs);
                    logDownloadProgress(50);
                    return aggs;
                }))
                .collect(Collectors.toList());
        logger.info("Downloading {} days", grouped.size());
        CompletableFuture<List<OHLCV>>[] futures = grouped.toArray(new CompletableFuture[grouped.size()]);
        return CompletableFuture.allOf(futures).thenApply(v ->
                grouped.stream()
                        .map(CompletableFuture::join)
                        .flatMap(List::stream)
            ).thenApply(aggs -> {
                stats.completeDownload();
                if (!tableName.isEmpty()) {
                    logger.info("Flushing {} candles to {}", stats.numRows, tableName);
                    QuestDBWriter.flushAggregates(tableName, aggs);
                }
                stats.completeFlush();
                return stats;
            }).join();
    }

    static void logDownloadProgress(int mod) {
        int i = agg1dCounter.incrementAndGet();
        if (i % mod == 0) {
            logger.info(i);
        }
    }

    @SuppressWarnings("unchecked")
    static BackfillRangeStats backfillRangeAggs(String tableName, Calendar from, Calendar to, List<String> tickers) {
        BackfillRangeStats stats = new BackfillRangeStats(tableName, from, to);

        if (tableName.isEmpty() || tableName.contains("agg1d")) {
            List<CompletableFuture<List<OHLCV>>> symbolAggs = tickers.stream()
                    .map(ticker -> CompletableFuture.supplyAsync(() -> {
                        List<OHLCV> aggs = PolygonClient.getAggsForSymbol(from, to, "day", ticker);
                        stats.completeRows(aggs);
                        logDownloadProgress(500);
                        return aggs;
                    }))
                    .collect(Collectors.toList());
            logger.info("Downloading {} tickers", symbolAggs.size());
            CompletableFuture<List<OHLCV>>[] futures = symbolAggs.toArray(new CompletableFuture[symbolAggs.size()]);
            return CompletableFuture.allOf(futures).thenApply(v ->
                    symbolAggs.stream()
                            .map(CompletableFuture::join)
                            .flatMap(List::stream)
            ).thenApply(aggs -> {
                stats.completeDownload();
                if (!tableName.isEmpty()) {
                    logger.info("Flushing {} candles to {}", stats.numRows, tableName);
                    QuestDBWriter.flushAggregates(tableName, aggs);
                }
                stats.completeFlush();
                return stats;
            }).join();
        }

        return null;
    }

    public static void saveTickers(List<Ticker> tickers) {
        try {
            FileOutputStream f = new FileOutputStream(new File("tickers.bin"));
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(tickers);

            o.close();
            f.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    public static List<Ticker> loadTickers() throws IOException {
        List<Ticker> res = null;
        try {
            FileInputStream fi = new FileInputStream(new File("tickers.bin"));
            ObjectInputStream oi = new ObjectInputStream(fi);

            res = (List<Ticker>) oi.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return res;
    }

    private static List<Ticker> getTickers() {
        logger.info("Loading tickers...");
        List<Ticker> tickers;
        try {
            tickers = loadTickers();
            PrintWriter out = new PrintWriter("tickers.csv");
            out.println("ticker");
            tickers.stream().map(t -> t.ticker).sorted().forEach(out::println);
        } catch (IOException e) {
            logger.info("Cached tickers not found.");
            logger.debug(e);
            logger.info("Downloading tickers...");
            tickers = PolygonClient.getTickers();
            saveTickers(tickers);
        }

        List<String> tickerStrings = tickers
                .stream()
                .map(ticker -> ticker.ticker)
                .sorted()
                .collect(Collectors.toList());

        String lastTicker = "";
        for (String ticker : tickerStrings) {
            if (lastTicker.compareTo(ticker) == 0) {
                System.err.println("Duplicate ticker: " + ticker);
                System.err.println("Delete tickers.bin and fix duplicate tickers");
                System.exit(1);
            }
            if (ticker.isEmpty()) {
                System.err.println("Empty ticker: " + ticker);
                System.err.println("Delete tickers.bin and fix duplicate tickers");
                System.exit(1);
            }
            lastTicker = ticker;
        }

        return tickers;
    }

    public static void backfill(String type, Calendar from, Calendar to, BackfillMethod method) {
        // Exchange active from 4:00 - 20:00 (https://polygon.io/blog/frequently-asked-questions/)
        // Maximum 5 trading days per week, 50000 rows per call
        int stepSize = type.equals("agg1m") ? PolygonClient.perPage * 7 / ((20 - 4) * 60 * 5) : 1;
        int stepType = Calendar.DATE;
        BackfillAllStats allStats = new BackfillAllStats();
        for (Calendar stepFrom = (Calendar) from.clone(); stepFrom.before(to); stepFrom.add(stepType, stepSize)) {
            if (stepSize == 1 && !MarketCalendar.isMarketOpen(stepFrom))
                continue;
            Calendar stepTo = (Calendar) stepFrom.clone();
            stepTo.add(stepType, stepSize);
            if (stepTo.after(to)) {
                stepTo = (Calendar) to.clone();
            }
            logger.info("Backfilling {} from {} to {}",
                    type,
                    sdf.format(stepFrom.getTime()),
                    sdf.format(stepTo.getTime()));

//            BackfillRangeStats dayStats = backfillRangeAggs(type, stepFrom, stepTo);
//            allStats.add(dayStats);
//            logger.info(dayStats);
        }
        logger.info(allStats);
    }

    public static BackfillAllStats backfillIndex(Calendar from, Calendar to, BackfillMethod method, String tableName) {
        BackfillAllStats allStats = new BackfillAllStats();
        List<String> tickers = method == BackfillMethod.aggs
                ? getTickers().stream().map(ticker -> ticker.ticker).collect(Collectors.toList())
                : null;
        // At maximum, 5 / 7 days are trading days
        int stepSize = method == BackfillMethod.aggs ? PolygonClient.perPage * 7 / 5 : 1;
        int stepType = method == BackfillMethod.aggs ? Calendar.DATE : Calendar.YEAR;

        for (Calendar stepFrom = (Calendar) from.clone(); stepFrom.before(to); stepFrom.add(stepType, stepSize)) {
            Calendar stepTo = (Calendar) stepFrom.clone();
            stepTo.add(stepType, stepSize);
            if (stepTo.after(to)) {
                stepTo = (Calendar) to.clone();
            }

            logger.info("Backfilling {} from {} to {}",
                    tableName,
                    sdf.format(stepFrom.getTime()),
                    sdf.format(stepTo.getTime()));

            BackfillRangeStats dayStats= method == BackfillMethod.aggs
                    ? backfillRangeAggs(tableName, stepFrom, stepTo, tickers)
                    : backfillRangeGrouped(tableName, stepFrom, stepTo);

            allStats.add(dayStats);
            logger.info(dayStats);
        }
        logger.info(allStats);
        return allStats;
    }
}
