package polygon.models;

public class Split extends DateFinancial {
    public String exDate;
    public String ticker;
    public String paymentDate;
    public Double ratio;
    public Double tofactor;
    public Double forfactor;

    @Override
    public String getDateString() {
        return exDate;
    }

    @Override
    public String getTicker() {
        return ticker;
    }
}
