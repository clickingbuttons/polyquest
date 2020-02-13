import aggregators.AggregateAllStats;
import aggregators.AggregateDayStats;
import aggregators.OHLCVAggregator;
import aggregators.TradeAggregator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.OHLCV;
import polygon.models.Trade;
import questdb.QuestDBReader;
import questdb.QuestDBWriter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

public class AggregateTradeRange {
    final static Logger logger = LogManager.getLogger(BackfillTradeRange.class);
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    static AggregateDayStats aggregateDay(Calendar day, List<String> aggregateLevels) {
        AggregateDayStats res = new AggregateDayStats(day);

        List<Trade> dayTrades = QuestDBReader.getTrades(day);
        logger.info("{} aggregating {} trades at {} levels", sdf.format(day.getTime()), dayTrades.size(), String.join(",", aggregateLevels));

        // dayTrades is sorted by symbol, time
        int count = 0;
        int lastIndex = 0;
        String lastSym = dayTrades.get(0).ticker;
        for (int i = 0; i < dayTrades.size(); i++) {
            String sym = dayTrades.get(i).ticker;
            if (sym.compareTo(lastSym) != 0) {
                List<OHLCV> agg1s = TradeAggregator.aggregate(dayTrades.subList(lastIndex, i), 1000000);
                List<OHLCV> agg1m = OHLCVAggregator.aggregate(agg1s, 60000000);
                QuestDBWriter.writeAggs(lastSym, "agg1s", agg1s);
                QuestDBWriter.writeAggs(lastSym, "agg1m", agg1m);
                lastSym = sym;
                lastIndex = i;
                count++;
            }
        }

        long startTime = System.currentTimeMillis();
        QuestDBWriter.flushAggregates();
        logger.info("Flushed {} symbols in {}s", count,  (System.currentTimeMillis() - startTime) /1000 );

        return res;
    }

    public static void aggregate(Calendar from, Calendar to, List<String> aggregateLevels) {
        logger.info("Aggregating from {} to {}", sdf.format(from.getTime()), sdf.format(to.getTime()));
        AggregateAllStats allStats = new AggregateAllStats();
        for (Calendar day = (Calendar) from.clone(); day.before(to); day.add(Calendar.DATE, 1)) {
            if (!MarketCalendar.isMarketOpen(day))
                continue;

            long startTime = System.currentTimeMillis();
            AggregateDayStats dayStats = aggregateDay(day, aggregateLevels);
            dayStats.timeElapsed = System.currentTimeMillis() - startTime;

            allStats.add(dayStats);
            logger.info(dayStats);
        }
        logger.info(allStats);
    }
}
