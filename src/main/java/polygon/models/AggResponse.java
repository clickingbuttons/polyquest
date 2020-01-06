package polygon.models;

import java.util.List;
import java.util.Map;

public class AggResponse {
    public String ticker;
    public String status;
    public int queryCount;
    public int resultsCount;
    public Boolean adjusted;
    public String aggType;
    public List<OHLCV> results;
}
