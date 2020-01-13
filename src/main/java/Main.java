import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.C;
import polygon.PolygonClient;
import polygon.models.Ticker;
import questdb.QuestDBReader;
import questdb.QuestDBWriter;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {
    final static Logger logger = LogManager.getLogger(Main.class);
    final static int threadCount = 200;
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static PolygonClient client = new PolygonClient();

    static BackfillDayStats backfillDay(Calendar day, List<String> tickerStrings) {
        BackfillDayStats res = new BackfillDayStats(day, tickerStrings.size());
        List<Runnable> tasks = new ArrayList<>();

        for (String ticker : tickerStrings) {
            tasks.add(new BackfillSymbolTask(day, ticker, res));
        }

        // Only about 1/4 of 33137 symbols have trades and will actually use CPU,
        // and the rest are just doing HTTP requests to check that there aren't any
        // trades on that day.
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
        QuestDBWriter.flushTrades();
        logger.info("Flushed in {}s", (System.currentTimeMillis() - startTime) /1000 );

        return res;
    }

    static void saveTickers(List<Ticker> tickers) {
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
    static List<Ticker> loadTickers() throws IOException {
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

    private static boolean isBadTicker(Ticker ticker) {
        // These don't work with Polygon most of the time anyways...
        if (ticker.ticker.length() > 5 || ticker.ticker.matches("^.*[^a-zA-Z].*$")) {
            return true;
        }

        return false;
    }

    private static boolean isWeekend(Calendar date) {
        int day = date.get(Calendar.DAY_OF_WEEK);
        if (day == Calendar.SATURDAY || day == Calendar.SUNDAY) {
            return true;
        }

        return false;
    }

    private static Calendar getEaster(int year) {
        // Anonymous Gregorian algorithm
        // https://en.wikipedia.org/wiki/Computus
        // https://math.stackexchange.com/questions/896954/decoding-gauss-easter-algorithm
        int a = year % 19,
            b = year / 100,
            c = year % 100,
            d = b / 4,
            e = b % 4,
            g = (8 * b + 13) / 25,
            h = (19 * a + b - d - g + 15) % 30,
            j = c / 4,
            k = c % 4,
            m = (a + 11 * h) / 319,
            r = (2 * e + 2 * j - k - h + m + 32) % 7,
            n = (h - m + r + 90) / 25,
            p = (h - m + r + n + 19) % 32;

        return new GregorianCalendar(year, n - 1, p);
    }

    private static boolean isMarketOpen(Calendar date) {
        int day = date.get(Calendar.DAY_OF_WEEK);
        int week = date.get(Calendar.DAY_OF_WEEK_IN_MONTH);
        int month = date.get(Calendar.MONTH);
        int year = date.get(Calendar.YEAR);

        // Weekend
        if (isWeekend(date)) {
            return false;
        }

        // New year's
        Calendar newYearDay = new GregorianCalendar(year, Calendar.JANUARY, 1);
        while (isWeekend(newYearDay)) {
            newYearDay.add(Calendar.DATE, 1);
        }
        if (date.compareTo(newYearDay) == 0) {
            return false;
        }

        // MLK day
        if (day == Calendar.MONDAY && week == 3 && month == Calendar.JANUARY) {
            return false;
        }

        // Washington's Birthday
        if (day == Calendar.MONDAY && week == 3 && month == Calendar.FEBRUARY) {
            return false;
        }

        // Good Friday
        Calendar easter = getEaster(year);
        easter.add(Calendar.DATE, -2);
        if (date.compareTo(easter) == 0) {
            return false;
        }

        // Memorial Day
        // Different date pre-1970, don't have to worry about this for now
        Calendar memorialDay = new GregorianCalendar(year, Calendar.MAY, 31);
        while (memorialDay.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY) {
            memorialDay.add(Calendar.DATE, -1);
        }
        if (date.compareTo(memorialDay) == 0) {
            return false;
        }

        // Independence Day
        Calendar independenceDay = new GregorianCalendar(year, Calendar.JULY, 4);
        if (independenceDay.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
            independenceDay.add(Calendar.DATE, -1);
        }
        else if (independenceDay.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
            independenceDay.add(Calendar.DATE, 1);
        }
        if (date.compareTo(independenceDay) == 0) {
            return false;
        }

        // Labor Day
        if (day == Calendar.MONDAY && week == 1 && month == Calendar.SEPTEMBER) {
            return false;
        }

        // Thanksgiving
        if (day == Calendar.THURSDAY && week == 4 && month == Calendar.NOVEMBER) {
            return false;
        }

        // Christmas
        Calendar christmasDay = new GregorianCalendar(year, Calendar.DECEMBER, 25);
        if (christmasDay.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
            christmasDay.add(Calendar.DATE, -1);
        }
        else if (christmasDay.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
            christmasDay.add(Calendar.DATE, 1);
        }
        if (date.compareTo(christmasDay) == 0) {
            return false;
        }

        // Hurricane Sandy
        Calendar hurricaneSandy = new GregorianCalendar(2012, Calendar.OCTOBER, 29);
        if (date.compareTo(hurricaneSandy) == 0) {
            return false;
        }

        Calendar hurricaneSandy2 = new GregorianCalendar(2012, Calendar.OCTOBER, 30);
        if (date.compareTo(hurricaneSandy2) == 0) {
            return false;
        }

        // George H.W. Bush death
        Calendar deadAt94 = new GregorianCalendar(2018, Calendar.DECEMBER, 05);
        if (date.compareTo(deadAt94) == 0) {
            return false;
	}

        return true;
    }

    private static void initTables() {
        QuestDBWriter.createTable("trades", "DAY");
//        QuestDBWriter.createTable("agg1s", "DAY");
//        QuestDBWriter.createTable("agg1m", "DAY");
//        QuestDBWriter.createTable("agg5m", "DAY");
//        QuestDBWriter.createTable("agg1h", "MONTH");
//        QuestDBWriter.createTable("agg1d", "NONE");
    }

    private static void backfill(Calendar from, Calendar to) {
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
        tickers.removeIf(Main::isBadTicker);
//        tickers.removeIf(ticker -> !ticker.ticker.equals("GME"));
        logger.info("Removed {} weird tickers. {} left.", prevSize - tickers.size(), tickers.size());

        List<String> tickerStrings = tickers
                .stream()
                .map(ticker -> ticker.ticker)
                .collect(Collectors.toList());

        BackfillAllStats allStats = new BackfillAllStats();
        for (Calendar it = (Calendar) from.clone(); it.before(to); it.add(Calendar.DATE, 1)) {
            if (!isMarketOpen((it)))
                continue;
            logger.info("Backfilling {} tickers on {} using {} threads",
                    tickerStrings.size(), sdf.format(it.getTime()), threadCount);

            long startTime = System.currentTimeMillis();
            BackfillDayStats dayStats = backfillDay(it, tickerStrings);
            dayStats.timeElapsed = System.currentTimeMillis() - startTime;

            allStats.add(dayStats);
            logger.info(dayStats);
        }
        logger.info(allStats);
    }

    public static void main(String args[]) {
        initTables();

        Calendar from = Calendar.getInstance();
        Calendar to = Calendar.getInstance();
        try {
            Date fromDate = QuestDBReader.getLastTrade().getTime();
            QuestDBReader.close();
            Date toDate = sdf.parse("2020-01-01");

            from.setTime(fromDate);
            to.setTime(toDate);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        backfill(from, to);
        QuestDBWriter.close();
        long duration = (System.currentTimeMillis() - startTime);
        logger.info("Took {} seconds", duration / 1000);
        System.exit(0);
    }
}
