package aggregators;

import polygon.models.OHLCV;
import polygon.models.TradeCondition;
import polygon.models.Trade;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class TradeAggregator {
//    final static Logger logger = LogManager.getLogger(TradeAggregator.class);

    public static List<OHLCV> aggregate(List<Trade> trades, int resolution) {
        List<OHLCV> res = new ArrayList<>();
        if (trades.size() == 0) return res;

        OHLCV curBucket = new OHLCV(trades.get(trades.size() - 1).time / resolution * resolution);
        // QuestDB gives us trades in reverse timestamp order when using `order by sym`...
        // Writing aggregator for increasing timestamps is easier than for decreasing
        for (int i = trades.size() - 1; i > -1; i--) {
            Trade t = trades.get(i);
            if (t.time / resolution > curBucket.timeMicros / resolution) {
                if (curBucket.open != 0) res.add(curBucket);
                curBucket = new OHLCV(t.time / resolution * resolution);
            }
            if (curBucket.open == 0 && !t.isUneligibleOpen()) {
//                logger.debug("{} open from tick {}", resolution, t);
                curBucket.open = t.price;
                curBucket.high = 0;
                curBucket.low = 0;
                curBucket.close = 0;
            }
            if (!t.isUneligibleHighLow()) {
                if (curBucket.high == 0 || curBucket.high < t.price) {
//                    logger.debug("{} high from tick {}", resolution, t);
                    curBucket.high = t.price;
                }
                if (curBucket.low == 0 || curBucket.low > t.price) {
//                    logger.debug("{} low from tick {}", resolution, t);
                    curBucket.low = t.price;
                }
            }
            if (!t.isUneligibleClose()) {
//                logger.debug("{} close from tick {}", resolution, t);
                curBucket.close = t.price;
            }
            curBucket.volume += t.size;
        }
        // Last bucket
        if (curBucket.open != 0) res.add(curBucket);

        return res;
    }

    public static OHLCV aggregateDay(List<Trade> trades, Calendar day) {
        if (trades.size() == 0) return null;
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar end = (Calendar) day.clone();
        end.set(Calendar.HOUR_OF_DAY, 16);

        OHLCV res = new OHLCV(end.getTimeInMillis());
        for (Trade t : trades) {
            if (!t.hasFlag(TradeCondition.FormTExtendedHours.condition) &&
                    !t.hasFlag(TradeCondition.FormTOutOfSequence.condition)) {
                if (res.open == 0 && !t.isUneligibleOpen()) {
//                    logger.debug("{} open from tick {}", sdf.format(day.getTime()), t);
                    res.open = t.price;
                    res.high = 0;
                    res.low = 0;
                    res.close = 0;
                }
                if (!t.isUneligibleHighLow()) {
                    if (res.high == 0 || res.high < t.price) {
//                        logger.debug("{} high from tick {}", sdf.format(day.getTime()), t);
                        res.high = t.price;
                    }
                    if (res.low == 0 || res.low > t.price) {
//                        logger.debug("{} low from tick {}", sdf.format(day.getTime()), t);
                        res.low = t.price;
                    }
                }
                if (!t.isUneligibleClose()) {
//                    logger.debug("{} close from tick {}", sdf.format(day.getTime()), t);
                    res.close = t.price;
                }
            }
            res.volume += t.size;
        }

        return res;
    }
}
