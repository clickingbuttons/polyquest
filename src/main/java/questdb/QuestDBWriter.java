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
import polygon.models.Dividend;
import polygon.models.OHLCV;
import polygon.models.Split;
import polygon.models.Trade;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QuestDBWriter {
    private final static Logger logger = LogManager.getLogger(QuestDBWriter.class);
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private final static CairoConfiguration configuration = new DefaultCairoConfiguration(
    System.getenv("QUESTDB_HOME") + "/db"
    );
    private final static CairoEngine engine =  new CairoEngine(configuration);
    private final static AllowAllSecurityContextFactory securityContextFactor = new AllowAllSecurityContextFactory();
    private final static CairoSecurityContext cairoSecurityContext = securityContextFactor.getInstance("admin");
    private static final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl();
    final static long dayMicros = 24 * 60 * 60 * 1000000L;

    private static String getPartitionType(String type) {
        if (type.contains("agg1d")) {
            return "YEAR";
        }

        return "DAY";
    }

    public static void createTable(String tableName) {
        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            String query = "";
            if (tableName.contains("trade")) {
                query = String.format(tradeTemplate, tableName);
            }
            else if (tableName.contains("dividend")) {
                query = String.format(dividendTemplate, tableName);
            }
            else if (tableName.contains("split")) {
                query = String.format(splitTemplate, tableName);
            }
            else if (tableName.contains("agg")) {
                String partitionType = getPartitionType(tableName);
                query = String.format(aggTemplate,
                        tableName,
                        partitionType.equals("YEAR") ? "    close_unadjusted DOUBLE, \n" : "",
                        partitionType
                );
            }
            compiler.compile(query, sqlExecutionContext);
        } catch (SqlException e) {
            if (e.getMessage().contains("table already exists")) {
                logger.info("Table {} already exists", tableName);
            } else {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static final String tradeTemplate = "CREATE TABLE %s (\n" +
            "    ts TIMESTAMP, \n" +
            "    sym SYMBOL CACHE INDEX, \n" +
            "    price DOUBLE, \n" +
            "    size INT, \n" +
            "    conditions INT, \n" +
            "    exchange BYTE \n" +
            ") TIMESTAMP(ts) PARTITION BY DAY";
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

    private static final String aggTemplate = "CREATE TABLE %s (\n" +
            "    ts TIMESTAMP, \n" +
            "    sym SYMBOL CACHE INDEX, \n" +
            "    open DOUBLE, \n" +
            "    high DOUBLE, \n" +
            "    low DOUBLE, \n" +
            "    close DOUBLE, \n" +
            "%s" +
            "    volume LONG\n" +
            ") TIMESTAMP(ts) PARTITION BY %s";
    public static void flushAggregates(String tableName, Stream<OHLCV> aggregates) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            aggregates.sorted().forEach(agg -> {
                TableWriter.Row row = writer.newRow(agg.timeMicros);
                row.putSym(1, agg.ticker);
                row.putDouble(2, agg.open);
                row.putDouble(3, agg.high);
                row.putDouble(4, agg.low);
                row.putDouble(5, agg.close);
                row.putLong(7, agg.volume);
                row.append();
            });
            writer.commit();
        }
    }

    private static long getMicros(String date) {
        long timeMicros = 0;
        try {
            timeMicros = sdf.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        timeMicros *= 1000;
        timeMicros -= timeMicros % dayMicros;
        return timeMicros;
    }

    private static final String dividendTemplate = "CREATE TABLE %s (\n" +
            "    ex_date TIMESTAMP, \n" +
            "    sym SYMBOL CACHE INDEX, \n" +
            "    payment_date TIMESTAMP, \n" +
            "    record_date TIMESTAMP, \n" +
            "    amount DOUBLE \n" +
            ") TIMESTAMP(ex_date) PARTITION BY YEAR";
    public static void flushDividends(String tableName, Stream<Dividend> dividends) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            dividends.sorted().forEach(div -> {
                TableWriter.Row row = writer.newRow(getMicros(div.exDate));
                row.putSym(1, div.ticker);
                if (div.paymentDate != null) {
                    row.putTimestamp(2, getMicros(div.paymentDate));
                }
                if (div.recordDate != null) {
                    row.putTimestamp(3, getMicros(div.recordDate));
                }
                row.putDouble(4, div.amount);
                row.append();
            });
            writer.commit();
        }
    }

    private static final String splitTemplate = "CREATE TABLE %s (\n" +
            "    ex_date TIMESTAMP, \n" +
            "    sym SYMBOL CACHE INDEX, \n" +
            "    payment_date TIMESTAMP, \n" +
            "    ratio DOUBLE \n" +
            ") TIMESTAMP(ex_date) PARTITION BY YEAR";
    public static void flushSplits(String tableName, Stream<Split> splits) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            splits.sorted().forEach(split -> {
                TableWriter.Row row = writer.newRow(getMicros(split.exDate));
                row.putSym(1, split.ticker);
                if (split.paymentDate != null) {
                    row.putTimestamp(2, getMicros(split.paymentDate));
                }
                row.putDouble(3, split.ratio);
                row.append();
            });
            writer.commit();
        }
    }

    public static void close() {
        engine.close();
    }
}
