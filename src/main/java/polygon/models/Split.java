package polygon.models;

public class Split extends DateFinancial {
    public String ticker;
    public String exDate;
    public String paymentDate;
    public double ratio;
    public double tofactor;
    public double forfactor;

    @Override
    public String getDateString() {
        return exDate;
    }

    @Override
    public String getTicker() {
        return ticker;
    }
}
