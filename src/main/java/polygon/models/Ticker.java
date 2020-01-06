package polygon.models;

import java.io.Serializable;

public class Ticker implements Serializable {
    public Ticker() {}
    public Ticker(
            String ticker,
            String name,
            String market,
            String locale,
            String currency,
            Boolean active,
            String primaryExch,
            String updated,
            TickerCodes codes,
            String url
    ) {
        this.ticker = ticker;
        this.name = name;
        this.market = market;
        this.locale = locale;
        this.currency = currency;
        this.active = active;
        this.primaryExch = primaryExch;
        this.updated = updated;
        this.codes = codes;
        this.url = url;
    }
    public String ticker;
    public String name;
    public String market;
    public String locale;
    public String currency;
    public Boolean active;
    public String primaryExch;
    public String updated;
    public TickerCodes codes;
    public String url;

    @Override
    public String toString() {
        return String.format("%s (%s) %b", ticker, primaryExch, active);
    }
}
