package backfill;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.PolygonClient;
import polygon.models.*;
import questdb.QuestDBReader;
import questdb.QuestDBWriter;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BackfillRange {
    final static Logger logger = LogManager.getLogger(BackfillRange.class);
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static AtomicInteger downloadCounter = new AtomicInteger(0);

    public enum BackfillMethod {
        grouped,
        aggs,
        dividends,
        splits,
        financials,
        trades
    }

    static List<String> getMarketDays(Calendar from, Calendar to) {
        List<String> marketDays = new ArrayList<>();
        for (Calendar day = (Calendar) from.clone(); day.before(to) || day.equals(to); day.add(Calendar.DATE, 1)) {
            if (!MarketCalendar.isMarketOpen(day))
                continue;
            marketDays.add(sdf.format(day.getTime()));
        }

        return marketDays;
    }

    static void logDownloadProgress(int mod, int size) {
        int i = downloadCounter.incrementAndGet();
        if (i % mod == 0) {
            logger.info("{} / {}", i, size);
        }
    }

    @SuppressWarnings("unchecked")
    static BackfillRangeStats backfillRangeByTickerOrDay(
            String tableName,
            Calendar from,
            Calendar to,
            List<String> tickersOrDays,
            BackfillMethod method
    ) {
        BackfillRangeStats stats = new BackfillRangeStats(tableName, from, to);

        downloadCounter.set(0);
        CompletableFuture<List<? extends DateFinancial>>[] futures = new CompletableFuture[tickersOrDays.size()];

        for (int i = 0; i < futures.length; i++) {
            final String tickerOrDay = tickersOrDays.get(i);
            futures[i] = CompletableFuture.supplyAsync(() -> {
                List<? extends DateFinancial> dateFinancials = null;
                switch (method) {
                    case grouped:
                        dateFinancials = PolygonClient.getAgg1d(tickerOrDay);
                        // For Jack don't filter tickers
                        if (!tableName.isEmpty()) {
                            dateFinancials.removeIf(agg -> !isValidTicker(agg.getTicker()));
                        }
                        if (dateFinancials.size() < 3000) {
                            logger.error("Holiday? {}", tickerOrDay);
                        }
                        break;
                    case aggs:
                        dateFinancials = PolygonClient.getAggsForSymbol(from, to, "day", tickerOrDay);
                        break;
                    case dividends:
                        dateFinancials = PolygonClient.getDividendsForSymbol(tickerOrDay);
                        break;
                    case splits:
                        dateFinancials = PolygonClient.getSplitsForSymbol(tickerOrDay);
                        break;
                    case financials:
                        dateFinancials = PolygonClient.getFinancialsForSymbol(tickerOrDay);
                        break;
                    case trades:
                        dateFinancials = PolygonClient.getTradesForSymbol(from, tickerOrDay);
                        break;
                }
                stats.completeDateFinancials(dateFinancials);
                logDownloadProgress(method == BackfillMethod.grouped ? 50 : 500, futures.length);
                return dateFinancials;
            });
        }
        return CompletableFuture.allOf(futures).thenApply(v ->
                Arrays.stream(futures)
                    .map(CompletableFuture::join)
                    .flatMap(List::stream)
        ).thenApply(dateFinancials -> {
            stats.completeDownload();
            if (!tableName.isEmpty()) {
                logger.info("Flushing {} {} to {}", stats.numRows, method.name(), tableName);
                switch (method) {
                    case grouped:
                        QuestDBWriter.flushAggregates(tableName, dateFinancials.map(OHLCV.class::cast));;
                        break;
                    case aggs:
                        QuestDBWriter.flushAggregates(tableName, dateFinancials.map(OHLCV.class::cast));;
                        break;
                    case dividends:
                        QuestDBWriter.flushDividends(tableName, dateFinancials.map(Dividend.class::cast));
                        break;
                    case splits:
                        QuestDBWriter.flushSplits(tableName, dateFinancials.map(Split.class::cast));
                        break;
                    case financials:
                        QuestDBWriter.flushFinancials(tableName, dateFinancials.map(Financial.class::cast));
                        break;
                    case trades:
                        QuestDBWriter.flushTrades(tableName, dateFinancials.map(Trade.class::cast));
                        break;
                }
            }
            stats.completeFlush();
            return stats;
        }).join();
    }

    static boolean isValidTicker(String ticker) {
        return ticker.matches("\\A[A-Za-z.-]+\\z") && ticker.length() <= 15;
    }

    private static boolean isToday(long epochMilli) {
        String today = sdf.format(new Date());
        String day = sdf.format(new Date(epochMilli));
        return day.equals(today);
    }

    // We used to use Polygon's tickers endpoint as a source of truth.
    // It's missing a lot of tickers, though. Use results from grouped endpoint instead.
    @SuppressWarnings("unchecked")
    public static List<Ticker> loadTickers() {
        try {
            File tickers = new File("tickers.bin");
            if (!isToday(tickers.lastModified())) {
                logger.info("Cached tickers are stale.");
                return null;
            }
            FileInputStream fi = new FileInputStream(tickers);
            ObjectInputStream oi = new ObjectInputStream(fi);

            return (List<Ticker>) oi.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // First run
            logger.info("Cached tickers not found.");
        }

        return null;
    }

    public static List<String> getTickers(String tableName) {
        List<String> tickers;

        if (tableName.isEmpty()) {
            File referenceTickers = new File("reference_tickers.csv");
            // if downloaded today already
            if (sdf.format(new Date(referenceTickers.lastModified())).equals(sdf.format(new Date()))) {
                logger.info("Loading tickers from {}...", referenceTickers.toString());
                tickers = new ArrayList<>(1 << 16);
                try (BufferedReader br = new BufferedReader(new FileReader(referenceTickers))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (!line.equals("ticker")) {
                            tickers.add(line);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                logger.info("Downloading reference tickers...");
                tickers = PolygonClient.getTickers().stream().map(ticker -> ticker.ticker).collect(Collectors.toList());
                // Cache for next run
                try {
                    PrintWriter out = new PrintWriter(referenceTickers);
                    out.println("ticker");
                    tickers.forEach(out::println);
                    out.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        } else {
            logger.info("Loading tickers from table agg1d...");
            tickers = QuestDBReader.getTickers();
        }

        return tickers;
    }

    public static void roundDownToDay(Calendar c) {
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
    }

    public static BackfillAllStats backfill(Calendar rangeFrom, Calendar rangeTo, BackfillMethod method, String tableName) {
        // We're always backfilling DAYS, so align from and to to nearest days
        roundDownToDay(rangeFrom);
        roundDownToDay(rangeTo);
        BackfillAllStats allStats = new BackfillAllStats();
        // At maximum, 5 / 7 days are trading days
        int stepSize = method == BackfillMethod.grouped || method == BackfillMethod.trades ? 1 : PolygonClient.perPage * 7 / 5;
        int stepType = method == BackfillMethod.grouped ? Calendar.YEAR : Calendar.DATE;

        for (Calendar from = (Calendar) rangeFrom.clone(); from.before(rangeTo); from.add(stepType, stepSize)) {
            Calendar to = (Calendar) from.clone();
            to.add(stepType, stepSize);
            if (to.after(rangeTo)) {
                to = (Calendar) rangeTo.clone();
            }

            if (method == BackfillMethod.trades && !MarketCalendar.isMarketOpen(from)) {
                continue;
            }
            logger.info("Backfilling {} from {} to {} into table {}",
                    method.name(),
                    sdf.format(from.getTime()),
                    sdf.format(to.getTime()),
                    tableName
            );

            List<String> tickersOrDays;
            if (method == BackfillMethod.grouped) {
                tickersOrDays = getMarketDays(from, to);
                logger.info("Downloading agg1d candles for {} days", tickersOrDays.size());
            }
            else if (method == BackfillMethod.trades) {
                tickersOrDays = QuestDBReader.getTickersOnDay(sdf.format(from.getTime()));
                logger.info("Downloading {} for {} tickers", method.name(), tickersOrDays.size());
            }
            else {
                tickersOrDays = getTickers(tableName);
                logger.info("Downloading {} for {} tickers", method.name(), tickersOrDays.size());
            }

            BackfillRangeStats dayStats = backfillRangeByTickerOrDay(tableName, from, to, tickersOrDays, method);
            allStats.add(dayStats);
            logger.info(dayStats);
        }
        logger.info(allStats);
        return allStats;
    }
}
