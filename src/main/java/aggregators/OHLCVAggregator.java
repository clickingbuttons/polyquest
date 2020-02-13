package aggregators;

import polygon.models.OHLCV;

import java.util.ArrayList;
import java.util.List;

public class OHLCVAggregator {
    public static List<OHLCV> aggregate(List<OHLCV> candles, long resolution) {
        List<OHLCV> res = new ArrayList<>();
        if (candles.size() > 0) {
            OHLCV curBucket = new OHLCV(candles.get(0).timeMicros / resolution * resolution);
            for (OHLCV c : candles) {
                if (c.timeMicros / resolution > curBucket.timeMicros / resolution) {
                    res.add(curBucket);
                    curBucket = new OHLCV(c.timeMicros / resolution * resolution);
                }

                if (curBucket.open == 0)
                    curBucket.open = c.open;
                if (curBucket.high == 0 || curBucket.high < c.high)
                    curBucket.high = c.high;
                if (curBucket.low == 0 || curBucket.low > c.low)
                    curBucket.low = c.low;
                curBucket.close = c.close;
                curBucket.volume += c.volume;
            }
            res.add(curBucket);
        }

        return res;
    }
}
