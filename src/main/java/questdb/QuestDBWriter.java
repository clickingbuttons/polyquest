package questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.OHLCV;
import polygon.models.Trade;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class QuestDBWriter {
    private final static Logger logger = LogManager.getLogger(QuestDBWriter.class);
    private final static CairoConfiguration configuration = new DefaultCairoConfiguration(
    System.getenv("QUESTDB_HOME") + "/db"
    );
    private final static CairoEngine engine =  new CairoEngine(configuration);
    private final static AllowAllSecurityContextFactory securityContextFactor = new AllowAllSecurityContextFactory();
    private final static CairoSecurityContext cairoSecurityContext = securityContextFactor.getInstance("admin");
    private final static SortedSet<Trade> writeCacheTrades = new ConcurrentSkipListSet<>();
    private final static Map<String, SortedSet<OHLCV>> writeCacheAggregates = new HashMap<>();

    public static void createTable(String tableName, String partitionType) {
        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            String query = "CREATE TABLE %s (\n" +
                    "    sym SYMBOL CACHE INDEX, \n" +
                    "    price DOUBLE, \n" +
                    "    size INT, \n" +
                    "    conditions INT, \n" +
                    "    exchange BYTE, \n" +
                    "    ts TIMESTAMP\n" +
                    ") TIMESTAMP(ts) PARTITION BY %s";
            if (tableName.contains("agg")) {
                query = "CREATE TABLE %s (\n" +
                        "    sym SYMBOL CACHE INDEX, \n" +
                        "    open DOUBLE, \n" +
                        "    high DOUBLE, \n" +
                        "    low DOUBLE, \n" +
                        "    close DOUBLE, \n" +
                        "    volume LONG, \n" +
                        "    ts TIMESTAMP\n" +
                        ") TIMESTAMP(ts) PARTITION BY %s";
                writeCacheAggregates.put(tableName, new ConcurrentSkipListSet<>());
            }
            String createTable = String.format(query, tableName, partitionType);
            compiler.compile(createTable);
        } catch (SqlException e) {
            if (e.getMessage().contains("table already exists")) {
                logger.info("Table {} already exists", tableName);
            } else {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    public static void writeTrades(String symbol, List<Trade> trades) {
        for (Trade t : trades) {
            t.ticker = symbol;
        }
        writeCacheTrades.addAll(trades);
    }

    public static void writeAggs(String symbol, String tableName, List<OHLCV> aggs) {
        for (OHLCV agg : aggs) {
            agg.ticker = symbol;
        }
        writeCacheAggregates.get(tableName).addAll(aggs);
    }

    public static void flushTrades(String tableName) {
        if (writeCacheTrades.size() > 0) {
            logger.info("Flushing {} trades", writeCacheTrades.size());
            try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
                for (Trade t : writeCacheTrades) {
                    long microSeconds = t.getTimeMicros();
                    TableWriter.Row row = writer.newRow(microSeconds);
                    row.putSym(0, t.ticker);
                    row.putDouble(1, t.price);
                    row.putInt(2, t.size);
                    row.putInt(3, t.encodeConditions());
                    row.putByte(4, t.encodeExchange());
                    row.append();
                }
                writer.commit();
            }
            writeCacheTrades.clear();
        }
    }

    public static void flushAggregates() {
        for (String tableName : writeCacheAggregates.keySet()) {
            Set<OHLCV> aggregates = writeCacheAggregates.get(tableName);
            logger.info("Flushing {} {} candles", aggregates.size(), tableName);
            try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
                for (OHLCV agg : aggregates) {
                    TableWriter.Row row = writer.newRow(agg.timeMicros);
                    row.putSym(0, agg.ticker);
                    row.putDouble(1, agg.open);
                    row.putDouble(2, agg.high);
                    row.putDouble(3, agg.low);
                    row.putDouble(4, agg.close);
                    row.putLong(5, agg.volume);
                    row.append();
                }
                writer.commit();
            }
            aggregates.clear();
        }
    }

    public static void close() {
        engine.close();
    }
}
