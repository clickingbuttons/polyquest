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
            Calendar from = QuestDBReader.getLastTrade("trades");
            Calendar to = new GregorianCalendar();
            to.add(-2, Calendar.DATE);
            BackfillTradeRange.backfill(from, to, 200);
        }
        else if (args[0].compareTo("aggregate") == 0) {
            List<String> aggregationLevels = new ArrayList<>();
            aggregationLevels.add("1s");
            aggregationLevels.add("1m");
            aggregationLevels.add("1d");
            QuestDBWriter.createTable("agg1s", "DAY");
            QuestDBWriter.createTable("agg1m", "DAY");
            QuestDBWriter.createTable("agg1d", "YEAR");
            Calendar from = QuestDBReader.getFirstTrade("trades");
            Calendar to = QuestDBReader.getLastTrade("trades");
            AggregateTradeRange.aggregate(from, to, aggregationLevels);
        }
        QuestDBWriter.close();
        QuestDBReader.close();
        logger.info("Took {} seconds", System.currentTimeMillis() - startTime / 1000);
    }
}
