import backfill.BackfillAllStats;
import backfill.BackfillDayStats;
import backfill.BackfillSymbolTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.PolygonClient;
import polygon.models.Ticker;
import questdb.QuestDBWriter;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BackfillTradeRange {
    final static Logger logger = LogManager.getLogger(BackfillTradeRange.class);
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static PolygonClient client = new PolygonClient();
    final static String tableName = "trades";

    static BackfillDayStats backfillDay(Calendar day, List<String> tickerStrings, int threadCount) {
        BackfillDayStats res = new BackfillDayStats(day, tickerStrings.size());
        List<Runnable> tasks = new ArrayList<>();

        for (String ticker : tickerStrings) {
            tasks.add(new BackfillSymbolTask(day, ticker, res));
        }

        // Only about 1/4 of 33676 symbols have trades.
        // The rest are just doing HTTP requests to check that there aren't any trades on that day.
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        for (Runnable r : tasks) {
            pool.execute(r);
        }

        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long startTime = System.currentTimeMillis();
        QuestDBWriter.flushTrades(tableName);
        logger.info("Flushed in {}s", (System.currentTimeMillis() - startTime) /1000 );

        return res;
    }

    public static void saveTickers(List<Ticker> tickers) {
        try {
            FileOutputStream f = new FileOutputStream(new File("tickers.bin"));
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(tickers);

            o.close();
            f.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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

    public static void backfill(Calendar from, Calendar to, int threadCount) {
        logger.info("Loading tickers...");
        List<Ticker> tickers;
        try {
            tickers = loadTickers();
        } catch (IOException e) {
            logger.info("Cached tickers not found.");
            logger.debug(e);
            logger.info("Downloading tickers...");
            tickers = client.getTickers();
            saveTickers(tickers);
        }
        int prevSize = tickers.size();
        tickers.removeIf(BackfillTradeRange::isBadTicker);
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
            if (ticker.isEmpty() || ticker == null) {
                System.err.println("Empty ticker: " + ticker);
                System.err.println("Delete tickers.bin and fix duplicate tickers");
                System.exit(1);
            }
            lastTicker = ticker;
        }

        BackfillAllStats allStats = new BackfillAllStats();
        for (Calendar day = (Calendar) from.clone(); day.before(to); day.add(Calendar.DATE, 1)) {
            if (!MarketCalendar.isMarketOpen(day))
                continue;
            logger.info("Backfilling {} tickers on {} using {} threads",
                    tickerStrings.size(), sdf.format(day.getTime()), threadCount);

            long startTime = System.currentTimeMillis();
            BackfillDayStats dayStats = backfillDay(day, tickerStrings, threadCount);
            dayStats.timeElapsed = System.currentTimeMillis() - startTime;

            allStats.add(dayStats);
            logger.info(dayStats);
        }
        logger.info(allStats);
    }
}
