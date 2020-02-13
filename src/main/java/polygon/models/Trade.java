package polygon.models;

import com.google.gson.annotations.SerializedName;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Trade implements Comparable<Trade> {
    public String ticker;
    // Nanoseconds when downloaded from Polygon. Microseconds when loaded from QuestDB.
    @SerializedName("t")
    public long time;
    // Participant/Exchange timestamp
    @SerializedName("y")
    public long exchangeTimeNanos;
    // Trade reporting facility timestamp
    @SerializedName("f")
    public long trfTimeNanos;
    @SerializedName("q")
    public int sequenceNumber;
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
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public boolean hasFlag(int flag) {
        if (conditions.contains(flag))
            return true;

        return false;
    }

    // https://www.ctaplan.com/publicdocs/ctaplan/notifications/trader-update/CTS_BINARY_OUTPUT_SPECIFICATION.pdf
    // PAGE 61
    public boolean isUneligibleOpen() {
        return  hasFlag(TradeCondition.AveragePrice.condition) ||
                hasFlag(TradeCondition.CashTrade.condition) ||
                hasFlag(TradeCondition.PriceVariation.condition) ||
                hasFlag(TradeCondition.OddLot.condition) ||
                hasFlag(TradeCondition.MarketCenterOfficialOpen.condition) || // Debatable
                hasFlag(TradeCondition.MarketCenterOfficialClose.condition) ||
                hasFlag(TradeCondition.NextDay.condition) ||
                hasFlag(TradeCondition.Seller.condition) ||
                hasFlag(TradeCondition.Contingent.condition) ||
                hasFlag(TradeCondition.ContingentQualified.condition) ||
                hasFlag(TradeCondition.CorrectedConsolidatedClosePrice.condition);
    }

    public boolean isUneligibleClose() {
        return  hasFlag(TradeCondition.AveragePrice.condition) ||
                hasFlag(TradeCondition.CashTrade.condition) ||
                hasFlag(TradeCondition.PriceVariation.condition) ||
                hasFlag(TradeCondition.OddLot.condition) ||
                hasFlag(TradeCondition.NextDay.condition) ||
                hasFlag(TradeCondition.MarketCenterOfficialOpen.condition) ||
                hasFlag(TradeCondition.MarketCenterOfficialClose.condition) || // Debatable
                hasFlag(TradeCondition.Seller.condition) ||
                hasFlag(TradeCondition.Contingent.condition) ||
                hasFlag(TradeCondition.ContingentQualified.condition);
    }

    public boolean isUneligibleHighLow() {
        return  hasFlag(TradeCondition.AveragePrice.condition) ||
                hasFlag(TradeCondition.CashTrade.condition) ||
                hasFlag(TradeCondition.PriceVariation.condition) ||
                hasFlag(TradeCondition.OddLot.condition) ||
                hasFlag(TradeCondition.NextDay.condition) ||
                hasFlag(TradeCondition.MarketCenterOfficialOpen.condition) || // Debatable
                hasFlag(TradeCondition.MarketCenterOfficialClose.condition) || // Debatable
                hasFlag(TradeCondition.Seller.condition) ||
                hasFlag(TradeCondition.Contingent.condition) ||
                hasFlag(TradeCondition.ContingentQualified.condition) ||
                hasFlag(TradeCondition.CorrectedConsolidatedClosePrice.condition); // Debatable
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

    public long getTimeMicros() {
        return (time + 500) / 1000;
    }

    public String toString() {
        return String.format("%d (%s) - %d @ %.3f", time, sdf.format(new Date(time / 1000)), size, price);
    }

    @Override
    public int compareTo(Trade t2) {
        // Can't return 0 if equal because used in Set
        if (t2.time >= time) {
            return -1;
        }
        return 1;
    }
}
