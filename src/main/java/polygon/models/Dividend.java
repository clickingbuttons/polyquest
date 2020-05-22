package polygon.models;

public class Dividend extends DateFinancial {
    public String ticker;
    public String exDate;
    public String paymentDate;
    public String recordDate;
    public double amount;

    @Override
    public String getDateString() {
        return exDate;
    }

    @Override
    public String getTicker() {
        return ticker;
    }
}
