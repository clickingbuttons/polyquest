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

import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class QuestDBReader {
    private final static Logger logger = LogManager.getLogger(QuestDBReader.class);
    private final static CairoConfiguration configuration = new DefaultCairoConfiguration(
    System.getenv("QUESTDB_HOME") + "/db"
    );
    private final static CairoEngine engine =  new CairoEngine(configuration);
    private final static AllowAllSecurityContextFactory securityContextFactor = new AllowAllSecurityContextFactory();

    public static Date getLastTrade() {
        long timestamp = 1262304000000000L;

        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            String query = "SELECT ts FROM trades LIMIT -1";
            RecordCursor cursor = compiler.compile(query).getRecordCursorFactory().getCursor();
            if (cursor.hasNext())
                timestamp = cursor.getRecord().getLong(0);
        } catch (SqlException e) {
            e.printStackTrace();
        }

        // UTC is 5 hours ahead
        return new Date(timestamp / 1000 + 19 * 60 * 60 * 1000);
    }

    public static void close() {
        engine.close();
    }
}
