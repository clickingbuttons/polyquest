package polygon.models;

import java.util.List;

public class TickerResponse {
    public int page;
    public int perPage;
    public int count;
    public String status;
    public List<Ticker> tickers;
}
