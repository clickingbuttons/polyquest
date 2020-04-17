package backfill;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.OHLCV;
import polygon.models.Trade;
import polygon.PolygonClient;
import questdb.QuestDBWriter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

public class BackfillSymbolTask implements Runnable {
    private Calendar from;
    private Calendar to;
    private String symbol;
    private String type;
    private BackfillRangeStats stats;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static Logger logger = LogManager.getLogger(BackfillSymbolTask.class);

    public BackfillSymbolTask(String type, Calendar from, Calendar to, String symbol, BackfillRangeStats stats) {
        this.from = (Calendar) from.clone();
        this.to = (Calendar) to.clone();
        this.symbol = symbol;
        this.stats = stats;
        this.type = type;
    }

    private void runTrades() {
        List<Trade> trades = PolygonClient.getTradesForSymbol(sdf.format(from.getTime()), symbol);
        // Sort trades by timestamp
        trades.sort((a, b) -> a.timeMicros < b.timeMicros ? 1 : 0);
        int numTrades = trades.size();

        if (numTrades > 0) {
            stats.symbolsWithData.add(symbol);
            logger.debug("{} {} had {} trades from {} to {}",
                    sdf.format(from.getTime()),
                    symbol,
                    numTrades,
                    trades.get(0).timeMicros / 1000,
                    trades.get(trades.size() - 1).timeMicros / 1000);

            QuestDBWriter.writeTrades(symbol, trades);
            stats.curNumRows.addAndGet(trades.size());
            trades.clear();
        }
        int num = stats.curNumSymbols.incrementAndGet();
        if (num % 500 == 0 || num == stats.numSymbols) {
            logger.info("{}: {} / {} ({} w/data)",
                    sdf.format(from.getTime()), num, stats.numSymbols, stats.symbolsWithData.size());
        }
    }

    private void runAgg(List<OHLCV> agg) {
        int numAggs = agg.size();
        if (numAggs > 0) {
            stats.symbolsWithData.add(symbol);
            logger.debug("{} {} had {} candles from {} to {}",
                    sdf.format(from.getTime()),
                    symbol,
                    numAggs,
                    agg.get(0).timeMicros,
                    agg.get(agg.size() - 1).timeMicros);

            QuestDBWriter.writeAggs(symbol, type, agg);
            stats.curNumRows.addAndGet(agg.size());
            agg.clear();
        }
        int num = stats.curNumSymbols.incrementAndGet();
        if (num % 500 == 0 || num == stats.numSymbols) {
            logger.info("{} to {}: {} / {} ({} w/data)",
                    sdf.format(from.getTime()), sdf.format(to.getTime()), num, stats.numSymbols, stats.symbolsWithData.size());
        }
    }

    public void run() {
        if (type.equals("trades")) {
            runTrades();
        }
        else if (type.equals("agg1m")) {
            List<OHLCV> agg1m = PolygonClient.getAggsForSymbol(from, to, "minute", symbol);
            runAgg(agg1m);
        }
        else if (type.equals("agg1d")) {
            List<OHLCV> agg1d = PolygonClient.getAggsForSymbol(from, to, "day", symbol);
            runAgg(agg1d);
        }
    }
}
