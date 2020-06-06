package polygon.models;

import com.google.gson.annotations.SerializedName;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Trade extends DateFinancial  {
    public String ticker;
    // EST microsecond timestamp
    @SerializedName("t")
    public long timeMicros;
    // Participant/Exchange timestamp
    @SerializedName("y")
    public long exchangeTimeNanos;
    // Trade reporting facility timestamp
    @SerializedName("f")
    public long trfTimeNanos;
    @SerializedName("q")
    public long sequenceNumber;
    @SerializedName("i")
    public String id;
    @SerializedName("x")
    public int exchange;
    @SerializedName("s")
    public int size;
    @SerializedName("c")
    public List<Integer> conditions = new ArrayList<>();
    @SerializedName("p")
    public double price;
    @SerializedName("z")
    public short tape;

    public boolean hasFlag(int flag) {
        if (conditions.contains(flag))
            return true;

        return false;
    }

    public boolean hasCondition(List<TradeCondition> conditions) {
        for (TradeCondition condition : conditions) {
            if (hasFlag(condition.condition)) {
                return true;
            }
        }

        return false;
    }

    public int encodeConditions() {
        // https://www.ctaplan.com/publicdocs/ctaplan/notifications/trader-update/CTS_BINARY_OUTPUT_SPECIFICATION.pdf
        // Page 33: Sale Condition 4 Char [ ]
        int res = 0;
        for (int i = 0; i < conditions.size(); i++) {
            res |= conditions.get(i) << (8 * i);
        }
        return res;
    }

    public byte encodeExchange() {
        // There are currently 34 exchanges https://api.polygon.io/v1/meta/exchanges
        // Take advantage of fact 34 < 128 and sneak in UTC tape (1-3) as well
        // T T X X X X X X
        // 1 1 0 0 0 0 0 1
        int res = ((tape << 6) | exchange);
        assert res < 128;
        return (byte) res;
    }

    public void decodeConditions(int encoded) {
        conditions.add(encoded & 0x000000FF);
        conditions.add((encoded & 0x0000FF00) >> 8);
        conditions.add((encoded & 0x00FF0000) >> 16);
        conditions.add((encoded & 0xFF000000) >> 24);
    }

    public void decodeExchange(byte encoded) {
        exchange = encoded & 0x3F;
        tape = (short) (encoded >> 6);
    }

    public String toString() {
        return String.format("%d (%s) - %d @ %.3f",
                timeMicros,
                Instant.ofEpochMilli(timeMicros / 1000).toString(),
                size,
                price
        );
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
