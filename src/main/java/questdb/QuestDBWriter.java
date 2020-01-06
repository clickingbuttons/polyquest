package questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.Trade;

import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class QuestDBWriter {
    private final static Logger logger = LogManager.getLogger(QuestDBWriter.class);
    private final static CairoConfiguration configuration = new DefaultCairoConfiguration(
    System.getenv("QUESTDB_HOME") + "/db"
    );
    private final static CairoEngine engine =  new CairoEngine(configuration);
    private final static AllowAllSecurityContextFactory securityContextFactor = new AllowAllSecurityContextFactory();
    private final static CairoSecurityContext cairoSecurityContext = securityContextFactor.getInstance("admin");
    private final static SortedSet<Trade> writeCache = new ConcurrentSkipListSet<>();

    public static void createTable(String tableName, String partitionType) {
        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            String query = "CREATE TABLE %s (\n" +
                    "    sym SYMBOL CACHE, \n" +
                    "    price FLOAT, \n" +
                    "    size INT, \n" +
                    "    conditions INT, \n" +
                    "    exchange BYTE, \n" +
                    "    ts TIMESTAMP\n" +
                    ") TIMESTAMP(ts) PARTITION BY %s";
            if (tableName.contains("agg")) {
                query = "CREATE TABLE %s (\n" +
                        "    sym SYMBOL CACHE, \n" +
                        "    open FLOAT, \n" +
                        "    high FLOAT, \n" +
                        "    low FLOAT, \n" +
                        "    close FLOAT, \n" +
                        "    volume INT, \n" +
                        "    ts TIMESTAMP\n" +
                        ") TIMESTAMP(ts) PARTITION BY %s";
            }
            compiler.compile(String.format(query, tableName, partitionType));
        } catch (SqlException e) {
            if (e.getMessage().contains("table already exists")) {
                logger.info("Table {} already exists", tableName);
            } else {
                e.printStackTrace();
            }
        }
    }

    public static void writeTrades(String symbol, List<Trade> trades) {
        for (Trade t : trades) {
            t.ticker = symbol;
        }
        writeCache.addAll(trades);
    }

    public static void flushTrades() {
        logger.info("Flushing {} trades", writeCache.size());
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, "trades")) {
            for (Trade t : writeCache) {
                long microSeconds = t.getTimeMicros();
                TableWriter.Row row = writer.newRow(microSeconds);
                row.putSym(0, t.ticker);
                row.putFloat(1, t.price);
                row.putInt(2, t.size);
                row.putInt(3, t.encodeConditions());
                row.putByte(4, t.encodeExchange());
                row.append();
            }
            writer.commit();
        }
        writeCache.clear();
    }

    public static void close() {
        engine.close();
    }
}
