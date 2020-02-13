package polygon.models;

import com.google.gson.annotations.SerializedName;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OHLCV implements Comparable<OHLCV> {
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

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public OHLCV(long t) {
        timeMicros = t;
    }

    public String toString() {
        return String.format("%d (%s) o: %4.3f h: %4.3f l: %4.3f c: %4.3f v: %d",
                timeMicros,
                sdf.format(new Date(timeMicros / 1000)),
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
