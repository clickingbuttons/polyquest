import backfill.BackfillAllStats;
import backfill.BackfillRange;
import questdb.QuestDBReader;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class Jack {
    public static void main(String args[]) {
        Calendar from = QuestDBReader.polygonStart;
        Calendar to = new GregorianCalendar();
        to.add(Calendar.DATE, -1);
//        Calendar from = new GregorianCalendar(2004, Calendar.JANUARY, 1);
//        Calendar to = new GregorianCalendar(2005, Calendar.JANUARY, 1);
        BackfillAllStats groupedStats = BackfillRange.backfillIndex(from, to, BackfillRange.BackfillMethod.grouped, "");
        groupedStats.writeSymbols("jack_agg1d_grouped.csv");
        groupedStats.writeJSON("jack_agg1d_grouped.json");
        BackfillAllStats aggStats = BackfillRange.backfillIndex(from, to, BackfillRange.BackfillMethod.aggs, "");
        aggStats.writeSymbols("jack_agg1d_aggs.csv");
        aggStats.writeJSON("jack_agg1d_aggs.json");
    }
}
