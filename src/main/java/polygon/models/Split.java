package polygon.models;

public class Split implements Comparable<Split> {
    public String ticker;
    public String exDate;
    public String paymentDate;
    public double ratio;
    public double tofactor;
    public double forfactor;

    @Override
    public int compareTo(Split other) {
        return exDate.compareTo(other.exDate);
    }
}
