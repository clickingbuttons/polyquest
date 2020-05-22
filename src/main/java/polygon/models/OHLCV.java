package polygon.models;

import com.google.gson.annotations.SerializedName;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public class OHLCV extends DateFinancial {
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
    public long getDate() {
        return timeMicros;
    }

    @Override
    public String getDateString() {
        return Instant.ofEpochMilli(timeMicros / 1000).toString().substring(0, 10);
    }

    @Override
    public String getTicker() {
        return ticker;
    }
}
