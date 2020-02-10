package backfill;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.Trade;
import polygon.PolygonClient;
import questdb.QuestDBWriter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

public class BackfillSymbolTask implements Runnable {
    private Calendar day;
    private String symbol;
    private BackfillDayStats stats;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static Logger logger = LogManager.getLogger(BackfillSymbolTask.class);

    public BackfillSymbolTask(Calendar day, String symbol, BackfillDayStats stats) {
        this.day = (Calendar) day.clone();
        this.symbol = symbol;
        this.stats = stats;
    }

    public void run() {
        List<Trade> trades = PolygonClient.getTradesForSymbol(sdf.format(day.getTime()), symbol);
        // Sort trades by timestamp
        trades.sort((a, b) -> a.timeNanos < b.timeNanos ? 1 : 0);
        int numTrades = trades.size();

        if (numTrades > 0) {
            stats.symbolsWithTrades.add(symbol);
            logger.debug("{} {} had {} trades from {} to {}",
                    sdf.format(day.getTime()),
                    symbol,
                    numTrades,
                    trades.get(0).timeNanos / 1000,
                    trades.get(trades.size() - 1).timeNanos / 1000);

//            OHLCV candle1d = TradeAggregator.aggregateDay(trades, day);
//            List<OHLCV> candles1s = TradeAggregator.aggregate(trades, 1000);
//            List<OHLCV> candles1m = OHLCVAggregator.aggregate(candles1s, 1000 * 60);
//            List<OHLCV> candles5m = OHLCVAggregator.aggregate(candles1m, 1000 * 60 * 5);
//            List<OHLCV> candles1h = OHLCVAggregator.aggregate(candles5m, 1000 * 60 * 60);

            QuestDBWriter.writeTrades(symbol, trades);
            stats.curNumTrades.addAndGet(trades.size());
            trades.clear();
        }

        int num = stats.curNumSymbols.incrementAndGet();
        if (num % 500 == 0 || num == stats.numSymbols) {
            logger.info("{}: {} / {} ({} w/trades)",
                    sdf.format(day.getTime()), num, stats.numSymbols, stats.symbolsWithTrades.size());
        }
    }
}
