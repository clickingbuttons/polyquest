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
    final static AtomicInteger agg1dCounter = new AtomicInteger(0);

    public enum BackfillMethod {
        grouped,
        aggs,
        dividends,
        splits,
        financials
    }

    @SuppressWarnings("unchecked")
    static BackfillRangeStats backfillRangeGrouped(String tableName, Calendar from, Calendar to) {
        BackfillRangeStats stats = new BackfillRangeStats(tableName, from, to);

        List<Calendar> marketDays = new ArrayList<>();
        for (Calendar day = (Calendar) from.clone(); day.before(to) || day.equals(to); day.add(Calendar.DATE, 1)) {
            if (!MarketCalendar.isMarketOpen(day))
                continue;
            marketDays.add((Calendar) day.clone());
        }
        logger.info("Downloading agg1d candles for {} days", marketDays.size());
        agg1dCounter.set(0);
        CompletableFuture<List<OHLCV>>[] futures = new CompletableFuture[marketDays.size()];
        for (int i = 0; i < futures.length; i++) {
            final Calendar marketDay = marketDays.get(i);
            futures[i] = CompletableFuture.supplyAsync(() -> {
                List<OHLCV> aggs = PolygonClient.getAgg1d(marketDay);
                aggs.removeIf(agg -> !isValidTicker(agg.ticker, tableName));
                stats.completeDateFinancials(aggs);
                logDownloadProgress(50);
                return aggs;
            });
        }

        return CompletableFuture.allOf(futures).thenApply(v ->
                Arrays.stream(futures)
                    .map(CompletableFuture::join)
                    .flatMap(List::stream)
                    .filter(agg -> isValidTicker(agg.ticker, tableName))
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
    static BackfillRangeStats backfillRangeByTicker(
            String tableName,
            Calendar from,
            Calendar to,
            List<String> tickers,
            BackfillMethod method
    ) {
        BackfillRangeStats stats = new BackfillRangeStats(tableName, from, to);

        logger.info("Downloading {} for {} tickers", method.name(), tickers.size());
        agg1dCounter.set(0);
        CompletableFuture<List<? extends DateFinancial>>[] futures = new CompletableFuture[tickers.size()];
        for (int i = 0; i < futures.length; i++) {
            final String ticker = tickers.get(i);
            futures[i] = CompletableFuture.supplyAsync(() -> {
                List<? extends DateFinancial> dateFinancials = null;
                if (method == BackfillMethod.aggs) {
                    dateFinancials = PolygonClient.getAggsForSymbol(from, to, "day", ticker);
                } else if (method == BackfillMethod.dividends) {
                    dateFinancials = PolygonClient.getDividendsForSymbol(ticker);
                } else if (method == BackfillMethod.splits) {
                    dateFinancials = PolygonClient.getSplitsForSymbol(ticker);
                } else if (method == BackfillMethod.financials) {
                    dateFinancials = PolygonClient.getFinancialsForSymbol(ticker);
                }
                stats.completeDateFinancials(dateFinancials);
                logDownloadProgress(500);
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
                logger.info("Flushing {} candles to {}", stats.numRows, tableName);
                if (method == BackfillMethod.aggs) {
                    QuestDBWriter.flushAggregates(tableName, dateFinancials.map(OHLCV.class::cast));;
                } else if (method == BackfillMethod.dividends) {
                    QuestDBWriter.flushDividends(tableName, dateFinancials.map(Dividend.class::cast));
                } else if (method == BackfillMethod.splits) {
                    QuestDBWriter.flushSplits(tableName, dateFinancials.map(Split.class::cast));
                } else if (method == BackfillMethod.financials) {
                    QuestDBWriter.flushFinancials(tableName, dateFinancials.map(Financial.class::cast));
                }
            }
            stats.completeFlush();
            return stats;
        }).join();
    }

    static boolean isValidTicker(String ticker, String tableName) {
        // For Jack don't filter tickers
        return tableName.isEmpty() || ticker.matches("\\A[A-Za-z.-]+\\z") && ticker.length() <= 15;
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

    public static BackfillAllStats backfill(Calendar from, Calendar to, BackfillMethod method, String tableName) {
        // We're always backfilling DAYS, so align from and to to nearest days
        roundDownToDay(from);
        roundDownToDay(to);
        BackfillAllStats allStats = new BackfillAllStats();
        // At maximum, 5 / 7 days are trading days
        int stepSize = method == BackfillMethod.grouped ? 1 : PolygonClient.perPage * 7 / 5;
        int stepType = method == BackfillMethod.grouped ? Calendar.YEAR : Calendar.DATE;

        for (Calendar stepFrom = (Calendar) from.clone(); stepFrom.before(to); stepFrom.add(stepType, stepSize)) {
            Calendar stepTo = (Calendar) stepFrom.clone();
            stepTo.add(stepType, stepSize);
            if (stepTo.after(to)) {
                stepTo = (Calendar) to.clone();
            }

            logger.info("Backfilling {} from {} to {} into table {}",
                    method.name(),
                    sdf.format(stepFrom.getTime()),
                    sdf.format(stepTo.getTime()),
                    tableName
            );

            BackfillRangeStats dayStats;
            if (method == BackfillMethod.grouped) {
                dayStats = backfillRangeGrouped(tableName, stepFrom, stepTo);
            } else {
                List<String> tickers = getTickers(tableName);
                dayStats = backfillRangeByTicker(tableName, stepFrom, stepTo, tickers, method);
            }

            allStats.add(dayStats);
            logger.info(dayStats);
        }
        logger.info(allStats);
        return allStats;
    }
}
