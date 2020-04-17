import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.PolygonClient;
import questdb.QuestDBReader;
import questdb.QuestDBWriter;

import java.util.*;

public class Main {
    final static Logger logger = LogManager.getLogger(Main.class);

    private static int getStepType(String type) {
        if (type.compareTo("agg1m") == 0) {
            return Calendar.MONTH;
        }
        else if (type.compareTo("agg1d") == 0) {
            return Calendar.YEAR;
        }

        return Calendar.DATE;
    }

    private static int getStep(String type) {
        if (type.compareTo("agg1m") == 0) {
            // Exchange active from 4:00 - 20:00 (https://polygon.io/blog/frequently-asked-questions/)
            // Maximum 23 trading days per month
            return PolygonClient.perPage / ((20 - 4) * 60 * 23);
        }
        else if (type.compareTo("agg1d") == 0) {
            // 263 max work days in year, 9 holidays minimum
            return PolygonClient.perPage / (263 - 9);
        }

        return 1;
    }

    private static String getPartitionType(String type) {
        if (type.compareTo("agg1d") == 0) {
            return "YEAR";
        }

        return "DAY";
    }

    public static void main(String args[]) {
        long startTime = System.currentTimeMillis();
        if (args[0].compareTo("backfill") == 0) {
            QuestDBWriter.createTable(args[1], getPartitionType(args[1]));
            Calendar from = QuestDBReader.getLastTimestamp(args[1]);
            Calendar to = new GregorianCalendar();
            to.set(Calendar.DATE, to.get(Calendar.DATE) - 2);
            BackfillRange.backfill(args[1], from, to, getStepType(args[1]), getStep(args[1]),  200);
        }
        else if (args[0].compareTo("aggregate") == 0) {
            List<String> aggregationLevels = new ArrayList<>();
            aggregationLevels.add("1s");
            aggregationLevels.add("1m");
            QuestDBWriter.createTable("agg1s", "DAY");
            QuestDBWriter.createTable("agg1m", "DAY");
            Calendar from = QuestDBReader.getLastTimestamp("agg1s");
            Calendar to = QuestDBReader.getLastTimestamp("trades");
//            Calendar to = new GregorianCalendar(2021, Calendar.FEBRUARY, 20);
//            Calendar from = new GregorianCalendar(2015, Calendar.JANUARY, 5);
            AggregateTradeRange.aggregate(from, to, aggregationLevels);
        }
        QuestDBWriter.close();
        QuestDBReader.close();
        logger.info("Took {} seconds", (System.currentTimeMillis() - startTime) / 1000);
    }
}
