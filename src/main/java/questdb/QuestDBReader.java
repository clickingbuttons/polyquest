package questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.Trade;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class QuestDBReader {
    private final static CairoConfiguration configuration = new DefaultCairoConfiguration(
    System.getenv("QUESTDB_HOME") + "/db"
    );
    private final static CairoEngine engine =  new CairoEngine(configuration);
    private final static AllowAllSecurityContextFactory securityContextFactor = new AllowAllSecurityContextFactory();
    private final static CairoSecurityContext cairoSecurityContext = securityContextFactor.getInstance("admin");

    public static Calendar getLastTrade(String tableName) {
        // Start of Polygon.io data
        Calendar res = new GregorianCalendar(2010, Calendar.FEBRUARY, 1);

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

    public static void close() {
        engine.close();
    }
}
