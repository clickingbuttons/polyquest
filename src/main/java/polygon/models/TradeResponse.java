package polygon.models;

import java.util.List;
import java.util.Map;

public class TradeResponse {
    public List<Trade> results;
    public Boolean success;
    public Map<String, Map<String, String>> map;
    public String ticker;
    public int results_count;
    public int db_latency;
}
