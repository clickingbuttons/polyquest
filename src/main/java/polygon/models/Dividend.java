package polygon.models;

public class Dividend extends DateFinancial {
    public String exDate;
    public String ticker;
    public String paymentDate;
    public String recordDate;
    public Double amount;

    @Override
    public String getDateString() {
        return exDate;
    }

    @Override
    public String getTicker() {
        return ticker;
    }
}
