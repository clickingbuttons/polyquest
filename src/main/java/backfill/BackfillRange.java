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
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BackfillRange {
    final static Logger logger = LogManager.getLogger(BackfillRange.class);
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    @SuppressWarnings("unchecked")
    static BackfillRangeStats backfillRange(String type, Calendar from, Calendar to) {
        long startTime = System.currentTimeMillis();

        // Use grouped market API for each day
        if (type.equals("agg1d")) {
            List<CompletableFuture<List<OHLCV>>> groupedDays = Stream
                    .iterate(from, day -> {
                        Calendar nextDay = (Calendar) day.clone();
                        nextDay.add(Calendar.DATE, 1);
                        return nextDay;
                    })
                    .limit(ChronoUnit.DAYS.between(from.toInstant(), to.toInstant()))
                    .filter(MarketCalendar::isMarketOpen)
                    .map(day -> CompletableFuture.supplyAsync(() -> PolygonClient.getAgg1d(day)))
                    .collect(Collectors.toList());
            logger.info("Downloading {} days", groupedDays.size());
            CompletableFuture<List<OHLCV>>[] futures = groupedDays.toArray(new CompletableFuture[groupedDays.size()]);
            return CompletableFuture.allOf(futures).thenApply(v ->
                    groupedDays.stream()
                            .map(CompletableFuture::join)
                            .flatMap(List::stream)
                            .sorted((a, b) -> a.timeMicros < b.timeMicros ? 1 : 0)
                            .collect(Collectors.toList())
                ).thenApply(aggs -> {
                    long startFlushTime = System.currentTimeMillis();
                    logger.info("Flushing {} {} candles", aggs.size(), type);
                    BackfillRangeStats stats = new BackfillRangeStats(
                            type,
                            from,
                            to,
                            System.currentTimeMillis() - startTime,
                            System.currentTimeMillis() - startFlushTime,
                            aggs.stream().map(agg -> agg.ticker).distinct().collect(Collectors.toList()),
                            aggs.size());
                    QuestDBWriter.flushAggregates(type, aggs);
                    aggs.clear();
                    return stats;
                }).join();
        }
        // Only about 1/4 of 33676 symbols have trades.
        // The rest are just doing HTTP requests to check that there aren't any trades on that day.
//        for (Ticker t : getAllTickers()) {
//            tasks.add(new BackfillSymbolTask(type, from, to, t.ticker, res));
//        }

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

    public static boolean isBadTicker(Ticker ticker) {
        // These don't work with Polygon most of the time anyways...
        if (ticker.ticker.length() > 5 || ticker.ticker.matches("^.*[^a-zA-Z].*$")) {
            return true;
        }

        return false;
    }

    private static List<Ticker> getAllTickers() {
        logger.info("Loading tickers...");
        List<Ticker> tickers;
        try {
            tickers = loadTickers();
        } catch (IOException e) {
            logger.info("Cached tickers not found.");
            logger.debug(e);
            logger.info("Downloading tickers...");
            tickers = PolygonClient.getTickers();
            saveTickers(tickers);
        }
        int prevSize = tickers.size();
        tickers.removeIf(BackfillRange::isBadTicker);
        logger.info("Removed {} weird tickers. {} left.", prevSize - tickers.size(), tickers.size());

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

    public static void backfill(String type, Calendar from, Calendar to) {
        // Exchange active from 4:00 - 20:00 (https://polygon.io/blog/frequently-asked-questions/)
        // Maximum 5 trading days per week
        int stepSize = type.equals("agg1m") ? PolygonClient.perPage * 7 / ((20 - 4) * 60 * 5) : 1;
        int stepType = type.equals("agg1d") ? Calendar.YEAR : Calendar.DATE;
        BackfillAllStats allStats = new BackfillAllStats();
        for (Calendar stepFrom = (Calendar) from.clone(); stepFrom.before(to); stepFrom.add(stepType, stepSize)) {
            if (stepType == Calendar.DATE && stepSize == 1 && !MarketCalendar.isMarketOpen(stepFrom))
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

            BackfillRangeStats dayStats = backfillRange(type, stepFrom, stepTo);

            allStats.add(dayStats);
            logger.info(dayStats);
        }
        logger.info(allStats);
    }
}
