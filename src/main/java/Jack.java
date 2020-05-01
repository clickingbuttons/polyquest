import backfill.BackfillAllStats;
import backfill.BackfillRange;
import questdb.QuestDBReader;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class Jack {
    public static void main(String args[]) {
        Calendar from = QuestDBReader.polygonStart;
        Calendar to = new GregorianCalendar();
//        Calendar from = new GregorianCalendar(2004, Calendar.JANUARY, 1);
//        Calendar to = new GregorianCalendar(2005, Calendar.JANUARY, 1);
        BackfillAllStats aggStats = BackfillRange.backfillIndex(from, to, BackfillRange.BackfillMethod.aggs, "");
        aggStats.writeSymbols("agg1d_aggs.csv");
        aggStats.writeJSON("agg1d_aggs_days.json");
        BackfillAllStats groupedStats = BackfillRange.backfillIndex(from, to, BackfillRange.BackfillMethod.grouped, "");
        groupedStats.writeSymbols("agg1d_grouped.csv");
        groupedStats.writeJSON("agg1d_grouped_days.json");
    }
}
