package polygon.models;

import questdb.QuestDBWriter;

// OHLCV, Dividend, Split, Financial
abstract public class DateFinancial implements Comparable<DateFinancial> {
    abstract public String getDateString();
    abstract public String getTicker();

    // For split, dividend, and financial
    public long getDate() {
        return QuestDBWriter.getMicros(getDateString());
    }

    @Override
    public int compareTo(DateFinancial other) {
        return Long.compare(getDate(), other.getDate());
    }
}
