package polygon.models;

import com.google.gson.annotations.SerializedName;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OHLCV implements Comparable<OHLCV> {
    @SerializedName("o")
    public double open = 0;
    @SerializedName("h")
    public double high = 0;
    @SerializedName("l")
    public double low = 0;
    @SerializedName("c")
    public double close = 0;
    @SerializedName("v")
    public int volume = 0;
    @SerializedName("t")
    public long timeMillis;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public OHLCV(long t) {
        timeMillis = t;
    }

    public String toString() {
        return String.format("%d (%s)\t\to: %4.3f h: %4.3f l: %4.3f c: %4.3f v: %d",
                timeMillis,
                sdf.format(new Date(timeMillis)),
                open,
                high,
                low,
                close,
                volume);
    }

    @Override
    public int compareTo(OHLCV o) {
        double openDiff = Math.abs(open - o.open);
        double highDiff = Math.abs(high - o.high);
        double lowDiff = Math.abs(low - o.low);
        double closeDiff = Math.abs(close - o.close);

        // (< $.02 off) OR (< 2% off)
        if ((openDiff < .02 || openDiff / o.open < .02) &&
                (highDiff < .02 || highDiff / o.high < .02) &&
                (lowDiff < .02 || lowDiff / o.low < .02) &&
                (closeDiff < .02 || closeDiff / o.close < .05) // Polygon gets this wrong a lot
                || volume < 10000 // 2019-01-18 ACY and other thinly traded stocks
//                && volume == o.volume
                && timeMillis == o.timeMillis)
            return 0;
        return -1;
    }
}
