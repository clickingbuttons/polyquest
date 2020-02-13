import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import questdb.QuestDBReader;
import questdb.QuestDBWriter;

import java.util.*;

public class Main {
    final static Logger logger = LogManager.getLogger(Main.class);

    public static void main(String args[]) {
        long startTime = System.currentTimeMillis();
        if (args[0].compareTo("backfill") == 0) {
            QuestDBWriter.createTable("trades", "DAY");
            Calendar from = QuestDBReader.getLastTimestamp("trades");
            Calendar to = new GregorianCalendar();
            BackfillTradeRange.backfill(from, to, 200);
        }
        else if (args[0].compareTo("aggregate") == 0) {
            List<String> aggregationLevels = new ArrayList<>();
            aggregationLevels.add("1s");
            aggregationLevels.add("1m");
            QuestDBWriter.createTable("agg1s", "DAY");
            QuestDBWriter.createTable("agg1m", "DAY");
            Calendar from = QuestDBReader.getLastTimestamp("agg1s");
            Calendar to = QuestDBReader.getLastTimestamp("trades");
            AggregateTradeRange.aggregate(from, to, aggregationLevels);
        }
        QuestDBWriter.close();
        QuestDBReader.close();
        logger.info("Took {} seconds", System.currentTimeMillis() - startTime / 1000);
    }
}
