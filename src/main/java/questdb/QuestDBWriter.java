package questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.OHLCV;
import polygon.models.Trade;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QuestDBWriter {
    private final static Logger logger = LogManager.getLogger(QuestDBWriter.class);
    private final static CairoConfiguration configuration = new DefaultCairoConfiguration(
    System.getenv("QUESTDB_HOME") + "/db"
    );
    private final static CairoEngine engine =  new CairoEngine(configuration);
    private final static AllowAllSecurityContextFactory securityContextFactor = new AllowAllSecurityContextFactory();
    private final static CairoSecurityContext cairoSecurityContext = securityContextFactor.getInstance("admin");
    private static final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl();

    private static String getPartitionType(String type) {
        if (type.contains("agg1d")) {
            return "YEAR";
        }

        return "DAY";
    }

    public static void createTable(String tableName) {
        String partitionType = getPartitionType(tableName);
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
            }
            String createTable = String.format(query, tableName, partitionType);
            compiler.compile(createTable, sqlExecutionContext);
        } catch (SqlException e) {
            if (e.getMessage().contains("table already exists")) {
                logger.info("Table {} already exists", tableName);
            } else {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    public static void flushTrades(String tableName, Collection<Trade> trades) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            for (Trade t : trades) {
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
    }

    public static void flushAggregates(String tableName, Stream<OHLCV> aggregates) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            aggregates.sorted().forEach(agg -> {
                TableWriter.Row row = writer.newRow(agg.timeMicros);
                row.putSym(0, agg.ticker);
                row.putDouble(1, agg.open);
                row.putDouble(2, agg.high);
                row.putDouble(3, agg.low);
                row.putDouble(4, agg.close);
                row.putLong(5, agg.volume);
                row.append();
            });
            writer.commit();
        }
    }

    public static void close() {
        engine.close();
    }
}
