package backfill;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.OHLCV;
import polygon.PolygonClient;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Callable;

public class BackfillSymbolTask implements Callable<List<OHLCV>> {
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

    @Override
    public List<OHLCV> call() {
        if (type.equals("agg1m")) {
            return PolygonClient.getAggsForSymbol(from, to, "minute", symbol);
        }
        return PolygonClient.getAggsForSymbol(from, to, "day", symbol);
    }
}
