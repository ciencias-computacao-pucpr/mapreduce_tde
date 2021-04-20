package util;

import org.apache.hadoop.shaded.com.google.common.base.Strings;

public class Transaction {
    private final String countryOrArea;
    private final int year;
    private final String commCode;
    private final String commodity;
    private final String flow;
    private final double tradeUsd;
    private final double weightKg;
    private final String quantityName;
    private final double quantity;
    private final String category;

    public Transaction(String line) {
        String[] values = line.split(";");

        // country_or_area
        this.countryOrArea = values[0];
        // year
        this.year = Integer.parseInt(values[1]);
        // comm_code
        this.commCode = values[2];
        // commodity
        this.commodity = values[3];
        // flow
        this.flow = values[4];
        // trade_usd
        this.tradeUsd = Strings.isNullOrEmpty(values[5]) ? 0.0 : Double.parseDouble(values[5]);
        // weight_kg
        this.weightKg = Strings.isNullOrEmpty(values[6]) ? 0.0 : Double.parseDouble(values[6]);
        // quantity_name
        this.quantityName = values[7];
        // quantity
        this.quantity = Strings.isNullOrEmpty(values[8]) ? 0 : Double.parseDouble(values[8]);
        // category
        this.category = values[9];
    }

    public static Transaction getInstanceNoHeadersNoTotals(String line) {
        if (Strings.isNullOrEmpty(line)) return null;
        if (line.equals(JobUtils.TXS_FILE_HEADER)) return null;

        Transaction ret = new Transaction(line);
        if (ret.getCommCode().equals("TOTAL")) return null;

        return ret;
    }

    public String getCountryOrArea() {
        return countryOrArea;
    }

    public int getYear() {
        return year;
    }

    public String getCommCode() {
        return commCode;
    }

    public String getCommodity() {
        return commodity;
    }

    public String getFlow() {
        return flow;
    }

    public double getTradeUsd() {
        return tradeUsd;
    }

    public double getWeightKg() {
        return weightKg;
    }

    public String getQuantityName() {
        return quantityName;
    }

    public double getQuantity() {
        return quantity;
    }

    public String getCategory() {
        return category;
    }
}
