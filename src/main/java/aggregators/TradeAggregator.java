package aggregators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.OHLCV;
import polygon.models.TradeCondition;
import polygon.models.Trade;

import java.util.ArrayList;
import java.util.List;

public class TradeAggregator {
    final static Logger logger = LogManager.getLogger(TradeAggregator.class);

    public static List<OHLCV> aggregate(List<Trade> trades, long resolution) {
        List<OHLCV> res = new ArrayList<>();
        if (trades.size() == 0) return res;

        OHLCV curBucket = new OHLCV(trades.get(0).timeMicros / resolution * resolution);
        for (Trade t : trades) {
            if (t.timeMicros / resolution > curBucket.timeMicros / resolution) {
                if (curBucket.open != 0) res.add(curBucket);
                curBucket = new OHLCV(t.timeMicros / resolution * resolution);
            }
            if (curBucket.open == 0) {
                logger.debug("{} open from tick {}", curBucket, t);
                curBucket.open = t.price;
                curBucket.high = 0;
                curBucket.low = 0;
                curBucket.close = 0;
            }
            if (t.hasCondition(TradeCondition.eligibleHighLow)) {
                if (curBucket.high == 0 || curBucket.high < t.price) {
                    logger.debug("{} high from tick {}", curBucket, t);
                    curBucket.high = t.price;
                }
                if (curBucket.low == 0 || curBucket.low > t.price) {
                    logger.debug("{} low from tick {}", curBucket, t);
                    curBucket.low = t.price;
                }

            }
            if (t.hasCondition(TradeCondition.eligibleLast)) {
                curBucket.close = t.price;
                logger.debug("{} close from tick {}", curBucket, t);
            }
            curBucket.volume += t.size;
        }
        // Last bucket
        if (curBucket.open != 0) res.add(curBucket);

        return res;
    }
}
