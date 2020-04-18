package polygon.models;

import com.google.gson.annotations.SerializedName;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public class OHLCV implements Comparable<OHLCV> {
    @SerializedName("T")
    public String ticker;
    @SerializedName("o")
    public double open = 0;
    @SerializedName("h")
    public double high = 0;
    @SerializedName("l")
    public double low = 0;
    @SerializedName("c")
    public double close = 0;
    @SerializedName("v")
    public long volume = 0;
    @SerializedName("t")
    public long timeMicros;
    @SerializedName("n")
    public long numTrades;

    public OHLCV(long t) {
        timeMicros = t;
    }

    public String toString() {
        return String.format("%d (%s) o: %4.3f h: %4.3f l: %4.3f c: %4.3f v: %d",
                timeMicros,
                Instant.ofEpochMilli(timeMicros / 1000).toString(),
                open,
                high,
                low,
                close,
                volume);
    }

    @Override
    public int compareTo(OHLCV o2) {
        // Can't return 0 if equal because used in Set
        if (o2.timeMicros >= timeMicros) {
            return -1;
        }
        return 1;
    }
}
