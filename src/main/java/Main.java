import aggregators.AggregateTradeRange;
import backfill.BackfillAllStats;
import backfill.BackfillRange;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import questdb.QuestDBReader;
import questdb.QuestDBWriter;

import java.util.*;

public class Main {
    final static Logger logger = LogManager.getLogger(Main.class);

    private static void backfillAgg1d(BackfillRange.BackfillMethod method, String tableName) {
        QuestDBWriter.createTable(tableName);
        Calendar from = QuestDBReader.getLastTimestamp(tableName);
        from.add(Calendar.DATE, 1);
        Calendar to = new GregorianCalendar();
        to.add(Calendar.DATE, -1);
        BackfillAllStats aggStats = BackfillRange.backfill(from, to, method, tableName);
        aggStats.writeSymbols(tableName + ".csv");
        aggStats.writeJSON(tableName + ".json");
    }

    private static void backfillIndex() {
//        backfillAgg1d(BackfillRange.BackfillMethod.aggs, "agg1d_aggs");
        backfillAgg1d(BackfillRange.BackfillMethod.grouped, "agg1d");
        backfillAgg1d(BackfillRange.BackfillMethod.dividends, "dividends");
        backfillAgg1d(BackfillRange.BackfillMethod.splits, "splits");
        backfillAgg1d(BackfillRange.BackfillMethod.financials, "financials");
    }

    private static void aggregate() {
        List<String> aggregationLevels = new ArrayList<>();
        aggregationLevels.add("1s");
        aggregationLevels.add("1m");
        QuestDBWriter.createTable("agg1s");
        QuestDBWriter.createTable("agg1m");
        Calendar from = QuestDBReader.getLastTimestamp("agg1s");
        Calendar to = QuestDBReader.getLastTimestamp("trades");
//            Calendar to = new GregorianCalendar(2021, Calendar.FEBRUARY, 20);
//            Calendar from = new GregorianCalendar(2015, Calendar.JANUARY, 5);
        AggregateTradeRange.aggregate(from, to, aggregationLevels);
    }

    private static void printUsage() {
        System.out.println("Usage: polyquest [backfill|aggregate] [index|agg1m|trades]");
        System.exit(1);
    }

    public static void main(String args[]) {
        long startTime = System.currentTimeMillis();
        if (args.length < 1) {
            printUsage();
        }
        if (args[0].equals("backfill")) {
            if (args.length < 2) {
                printUsage();
            }
            backfillIndex();
        }
        else if (args[0].equals("aggregate")) {
            aggregate();
        }
        QuestDBWriter.close();
        QuestDBReader.close();
        logger.info("Took {} seconds", (System.currentTimeMillis() - startTime) / 1000);
    }
}
