import aggregators.AggregateAllStats;
import aggregators.AggregateDayStats;
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
        logger.info("Aggregating {} trades", dayTrades.size());

        // dayTrades is sorted by symbol, time
        List<OHLCV> agg1s = TradeAggregator.aggregate(dayTrades, 1000);
        QuestDBWriter.writeAggs(agg1s);

        long startTime = System.currentTimeMillis();
        QuestDBWriter.flushAggregates("agg1s");
        logger.info("Flushed in {}s", (System.currentTimeMillis() - startTime) /1000 );

        return res;
    }

    public static void aggregate(Calendar from, Calendar to, List<String> aggregateLevels) {
        logger.info("Aggregating from {} to {}", sdf.format(from.getTime()), sdf.format(to.getTime()));
        AggregateAllStats allStats = new AggregateAllStats();
        for (Calendar day = (Calendar) from.clone(); day.before(to); day.add(Calendar.DATE, 1)) {
            if (!MarketCalendar.isMarketOpen(day))
                continue;

            logger.info("Aggregating {} levels on {}", String.join(",", aggregateLevels), sdf.format(day.getTime()));

            long startTime = System.currentTimeMillis();
            AggregateDayStats dayStats = aggregateDay(day, aggregateLevels);
            dayStats.timeElapsed = System.currentTimeMillis() - startTime;

            allStats.add(dayStats);
            logger.info(dayStats);
        }
        logger.info(allStats);
    }
}
