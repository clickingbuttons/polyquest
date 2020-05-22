package polygon.models;

public class Dividend implements Comparable<Dividend> {
    public String ticker;
    public String exDate;
    public String paymentDate;
    public String recordDate;
    public double amount;

    @Override
    public int compareTo(Dividend other) {
        return exDate.compareTo(other.exDate);
    }
}
