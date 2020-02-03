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
                        "    volume INT, \n" +
                        "    ts TIMESTAMP\n" +
                        ") TIMESTAMP(ts) PARTITION BY %s";
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
        writeCache.addAll(trades);
    }

    public static void flushTrades(String tableName) {
        logger.info("Flushing {} trades", writeCache.size());
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            for (Trade t : writeCache) {
                long microSeconds = t.getTimeMicros() - (5 * 60 * 60 * 1000000L);
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
