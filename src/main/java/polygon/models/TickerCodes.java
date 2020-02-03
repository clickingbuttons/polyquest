package polygon.models;

import java.io.Serializable;

public class TickerCodes implements Serializable {
    public String cik;
    public String figiuid;
    public String scfigi;
    public String cfigi;
    public String figi;

    public TickerCodes() {}
    public TickerCodes(String cik, String figiuid, String scfigi, String cfigi, String figi) {
        this.cik = cik;
        this.figiuid = figiuid;
        this.scfigi = scfigi;
        this.cfigi = cfigi;
        this.figi = figi;
    }
}
