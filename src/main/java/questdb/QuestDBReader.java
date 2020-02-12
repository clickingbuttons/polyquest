package questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import polygon.models.Trade;

import java.text.SimpleDateFormat;
import java.util.*;

public class QuestDBReader {
    private final static CairoConfiguration configuration = new DefaultCairoConfiguration(
    System.getenv("QUESTDB_HOME") + "/db"
    );
    private final static CairoEngine engine =  new CairoEngine(configuration);
    private final static AllowAllSecurityContextFactory securityContextFactor = new AllowAllSecurityContextFactory();
    private final static CairoSecurityContext cairoSecurityContext = securityContextFactor.getInstance("admin");
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    // Start of Polygon.io data
    private final static Calendar polygonStart = new GregorianCalendar(2010, Calendar.JANUARY, 1);

    public static Calendar getLastTimestamp(String tableName) {
        Calendar res = (Calendar) polygonStart.clone();

        TableReader reader = engine.getReader(cairoSecurityContext, tableName);
        long tableMicros = reader.getMaxTimestamp();
        long startMicros = res.getTimeInMillis() * 1000;
        if (tableMicros > startMicros) {
            // We store data in EST, add 5 hours for GMT
            res.setTime(new Date(tableMicros / 1000 + (24 - 5) * 60 * 60 * 1000));
            res.set(Calendar.HOUR_OF_DAY, 0);
            res.set(Calendar.MINUTE, 0);
            res.set(Calendar.SECOND, 0);
            res.set(Calendar.MILLISECOND, 0);
        }

        return res;
    }

    public static Calendar getFirstTimestamp(String tableName) {
        Calendar res = (Calendar) polygonStart.clone();

        TableReader reader = engine.getReader(cairoSecurityContext, tableName);
        long tableMicros = reader.getMinTimestamp();
        long startMicros = res.getTimeInMillis() * 1000;
        if (tableMicros < startMicros) {
            res.setTime(new Date(tableMicros / 1000));
            res.set(Calendar.HOUR_OF_DAY, 0);
            res.set(Calendar.MINUTE, 0);
            res.set(Calendar.SECOND, 0);
            res.set(Calendar.MILLISECOND, 0);
        }

        return res;
    }

    public static List<Trade> getTrades(Calendar date) {
        List<Trade> res = new ArrayList<>();
        CairoEngine engine = new CairoEngine(configuration);
        SqlCompiler compiler = new SqlCompiler(engine);
        String formatted = sdf.format(date.getTime());
        String query = String.format("select *" +
                        " from trades" +
                        " where ts>'%sT00:00:00.000Z' and ts<'%sT23:59:59.999Z'" +
                        " order by sym", formatted, formatted);
        try (RecordCursor cursor = compiler.compile(query).getRecordCursorFactory().getCursor()) {
            Record record = cursor.getRecord();

            while (cursor.hasNext()) {
                Trade t = new Trade();
                t.ticker = record.getSym(0).toString();
                t.price = record.getFloat(1);
                t.size = record.getInt(2);
                t.decodeConditions(record.getInt(3));
                t.exchange = record.getByte(4);
                t.time = record.getTimestamp(5);
                res.add(t);
            }
        } catch (SqlException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return res;
    }

    public static void close() {
        engine.close();
    }
}
