package questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QuestDBWriter {
    private final static Logger logger = LogManager.getLogger(QuestDBWriter.class);
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private final static CairoConfiguration configuration = new DefaultCairoConfiguration(
    System.getenv("QUESTDB_HOME") + "/db"
    );
    private final static CairoEngine engine =  new CairoEngine(configuration);
    private final static AllowAllSecurityContextFactory securityContextFactor = new AllowAllSecurityContextFactory();
    private final static CairoSecurityContext cairoSecurityContext = securityContextFactor.getInstance("admin");
    private static final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(configuration, null, 1);
    final static long dayMicros = 24 * 60 * 60 * 1000000L;

    private static String getPartitionType(String type) {
        if (type.contains("agg1d")) {
            return "YEAR";
        }

        return "DAY";
    }

    public static void createTable(String tableName) {
        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            String query = "";
            if (tableName.contains("trade")) {
                query = String.format(tradeTemplate, tableName);
            }
            else if (tableName.contains("dividend")) {
                query = String.format(dividendTemplate, tableName);
            }
            else if (tableName.contains("split")) {
                query = String.format(splitTemplate, tableName);
            }
            else if (tableName.contains("financial")) {
                query = String.format(financialTemplate, tableName);
            }
            else if (tableName.contains("agg")) {
                String partitionType = getPartitionType(tableName);
                query = String.format(aggTemplate,
                        tableName,
                        partitionType.equals("YEAR") ? "    close_unadjusted DOUBLE, \n" : "",
                        partitionType
                );
            }
            compiler.compile(query, sqlExecutionContext);
        } catch (SqlException e) {
            if (e.getMessage().contains("table already exists")) {
                logger.info("Table {} already exists", tableName);
            } else {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static final String tradeTemplate = "CREATE TABLE %s (\n" +
            "    ts TIMESTAMP, \n" +
            "    sym SYMBOL CACHE INDEX, \n" +
            "    price DOUBLE, \n" +
            "    size INT, \n" +
            "    conditions INT, \n" +
            "    exchange BYTE \n" +
            ") TIMESTAMP(ts) PARTITION BY DAY";
    public static void flushTrades(String tableName, Stream<Trade> trades) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            trades.sorted().forEach(t -> {
                TableWriter.Row row = writer.newRow(t.timeMicros);
                row.putSym(1, t.ticker);
                row.putDouble(2, t.price);
                row.putInt(3, t.size);
                row.putInt(4, t.encodeConditions());
                row.putByte(5, t.encodeExchange());
                row.append();
            });
            writer.commit();
        }
    }

    private static final String aggTemplate = "CREATE TABLE %s (\n" +
            "    ts TIMESTAMP, \n" +
            "    sym SYMBOL CACHE INDEX, \n" +
            "    open DOUBLE, \n" +
            "    high DOUBLE, \n" +
            "    low DOUBLE, \n" +
            "    close DOUBLE, \n" +
            "%s" +
            "    volume LONG\n" +
            ") TIMESTAMP(ts) PARTITION BY %s";
    public static void flushAggregates(String tableName, Stream<OHLCV> aggregates) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            aggregates.sorted().collect(Collectors.toList()).forEach(agg -> {
                TableWriter.Row row = writer.newRow(agg.timeMicros);
                row.putSym(1, agg.ticker);
                row.putDouble(2, agg.open);
                row.putDouble(3, agg.high);
                row.putDouble(4, agg.low);
                row.putDouble(5, agg.close);
                row.putLong(7, agg.volume);
                row.append();
            });
            writer.commit();
        }
    }

    public static long getMicros(String date) {
        long timeMicros = 0;
        try {
            timeMicros = sdf.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        timeMicros *= 1000;
        timeMicros -= timeMicros % dayMicros;
        return timeMicros;
    }

    private static final String dividendTemplate = "CREATE TABLE %s (\n" +
            "    exDate TIMESTAMP, \n" +
            "    sym SYMBOL CACHE INDEX, \n" +
            "    paymentDate TIMESTAMP, \n" +
            "    recordDate TIMESTAMP, \n" +
            "    amount DOUBLE \n" +
            ") TIMESTAMP(exDate) PARTITION BY YEAR";
    public static void flushDividends(String tableName, Stream<Dividend> dividends) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            dividends.sorted().forEach(div -> {
                TableWriter.Row row = writer.newRow(div.getDate());
                row.putSym(1, div.ticker);
                if (div.paymentDate != null) {
                    row.putTimestamp(2, getMicros(div.paymentDate));
                }
                if (div.recordDate != null) {
                    row.putTimestamp(3, getMicros(div.recordDate));
                }
                row.putDouble(4, div.amount);
                row.append();
            });
            writer.commit();
        }
    }

    private static final String splitTemplate = "CREATE TABLE %s (\n" +
            "    exDate TIMESTAMP, \n" +
            "    sym SYMBOL CACHE INDEX, \n" +
            "    paymentDate TIMESTAMP, \n" +
            "    ratio DOUBLE \n" +
            ") TIMESTAMP(exDate) PARTITION BY YEAR";
    public static void flushSplits(String tableName, Stream<Split> splits) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            splits.sorted().forEach(split -> {
                TableWriter.Row row = writer.newRow(split.getDate());
                row.putSym(1, split.ticker);
                putTimestamp(row, 2, split.paymentDate);
                row.putDouble(3, split.ratio);
                row.append();
            });
            writer.commit();
        }
    }

    private static void putTimestamp(TableWriter.Row row, int index, String date) {
        if (date != null) {
            row.putTimestamp(index, getMicros(date));
        }
    }

    private static void putDouble(TableWriter.Row row, int index, Double value) {
        if (value != null) {
            row.putDouble(index, value);
        }
    }

    private static final String financialTemplate = "CREATE TABLE %s (\n" +
            "    dateKey TIMESTAMP, \n" +
            "    sym SYMBOL CACHE INDEX, \n" +
            "    period SYMBOL, \n" +
            "    calendarDate TIMESTAMP, \n" +
            "    reportPeriod TIMESTAMP, \n" +
            "    updated TIMESTAMP, \n" +
            "    accumulatedOtherComprehensiveIncome DOUBLE, \n" +
            "    assets DOUBLE, \n" +
            "    assetsAverage DOUBLE, \n" +
            "    assetsCurrent DOUBLE, \n" +
            "    assetTurnover DOUBLE, \n" +
            "    assetsNonCurrent DOUBLE, \n" +
            "    bookValuePerShare DOUBLE, \n" +
            "    capitalExpenditure DOUBLE, \n" +
            "    cashAndEquivalents DOUBLE, \n" +
            "    cashAndEquivalentsUSD DOUBLE, \n" +
            "    costOfRevenue DOUBLE, \n" +
            "    consolidatedIncome DOUBLE, \n" +
            "    currentRatio DOUBLE, \n" +
            "    debtToEquityRatio DOUBLE, \n" +
            "    debt DOUBLE, \n" +
            "    debtCurrent DOUBLE, \n" +
            "    debtNonCurrent DOUBLE, \n" +
            "    debtUSD DOUBLE, \n" +
            "    deferredRevenue DOUBLE, \n" +
            "    depreciationAmortizationAndAccretion DOUBLE, \n" +
            "    deposits DOUBLE, \n" +
            "    dividendYield DOUBLE, \n" +
            "    dividendsPerBasicCommonShare DOUBLE, \n" +
            "    earningBeforeInterestTaxes DOUBLE, \n" +
            "    earningsBeforeInterestTaxesDepreciationAmortization DOUBLE, \n" +
            "    EBITDAMargin DOUBLE, \n" +
            "    earningsBeforeInterestTaxesDepreciationAmortizationUSD DOUBLE, \n" +
            "    earningBeforeInterestTaxesUSD DOUBLE, \n" +
            "    earningsBeforeTax DOUBLE, \n" +
            "    earningsPerBasicShare DOUBLE, \n" +
            "    earningsPerDilutedShare DOUBLE, \n" +
            "    earningsPerBasicShareUSD DOUBLE, \n" +
            "    shareholdersEquity DOUBLE, \n" +
            "    averageEquity DOUBLE, \n" +
            "    shareholdersEquityUSD DOUBLE, \n" +
            "    enterpriseValue DOUBLE, \n" +
            "    enterpriseValueOverEBIT DOUBLE, \n" +
            "    enterpriseValueOverEBITDA DOUBLE, \n" +
            "    freeCashFlow DOUBLE, \n" +
            "    freeCashFlowPerShare DOUBLE, \n" +
            "    foreignCurrencyUSDExchangeRate DOUBLE, \n" +
            "    grossProfit DOUBLE, \n" +
            "    grossMargin DOUBLE, \n" +
            "    goodwillAndIntangibleAssets DOUBLE, \n" +
            "    interestExpense DOUBLE, \n" +
            "    investedCapital DOUBLE, \n" +
            "    investedCapitalAverage DOUBLE, \n" +
            "    inventory DOUBLE, \n" +
            "    investments DOUBLE, \n" +
            "    investmentsCurrent DOUBLE, \n" +
            "    investmentsNonCurrent DOUBLE, \n" +
            "    totalLiabilities DOUBLE, \n" +
            "    currentLiabilities DOUBLE, \n" +
            "    liabilitiesNonCurrent DOUBLE, \n" +
            "    marketCapitalization DOUBLE, \n" +
            "    netCashFlow DOUBLE, \n" +
            "    netCashFlowBusinessAcquisitionsDisposals DOUBLE, \n" +
            "    issuanceEquityShares DOUBLE, \n" +
            "    issuanceDebtSecurities DOUBLE, \n" +
            "    paymentDividendsOtherCashDistributions DOUBLE, \n" +
            "    netCashFlowFromFinancing DOUBLE, \n" +
            "    netCashFlowFromInvesting DOUBLE, \n" +
            "    netCashFlowInvestmentAcquisitionsDisposals DOUBLE, \n" +
            "    netCashFlowFromOperations DOUBLE, \n" +
            "    effectOfExchangeRateChangesOnCash DOUBLE, \n" +
            "    netIncome DOUBLE, \n" +
            "    netIncomeCommonStock DOUBLE, \n" +
            "    netIncomeCommonStockUSD DOUBLE, \n" +
            "    netLossIncomeFromDiscontinuedOperations DOUBLE, \n" +
            "    netIncomeToNonControllingInterests DOUBLE, \n" +
            "    profitMargin DOUBLE, \n" +
            "    operatingExpenses DOUBLE, \n" +
            "    operatingIncome DOUBLE, \n" +
            "    tradeAndNonTradePayables DOUBLE, \n" +
            "    payoutRatio DOUBLE, \n" +
            "    priceToBookValue DOUBLE, \n" +
            "    priceEarnings DOUBLE, \n" +
            "    priceToEarningsRatio DOUBLE, \n" +
            "    propertyPlantEquipmentNet DOUBLE, \n" +
            "    preferredDividendsIncomeStatementImpact DOUBLE, \n" +
            "    sharePriceAdjustedClose DOUBLE, \n" +
            "    priceSales DOUBLE, \n" +
            "    priceToSalesRatio DOUBLE, \n" +
            "    tradeAndNonTradeReceivables DOUBLE, \n" +
            "    accumulatedRetainedEarningsDeficit DOUBLE, \n" +
            "    revenues DOUBLE, \n" +
            "    revenuesUSD DOUBLE, \n" +
            "    researchAndDevelopmentExpense DOUBLE, \n" +
            "    returnOnAverageAssets DOUBLE, \n" +
            "    returnOnAverageEquity DOUBLE, \n" +
            "    returnOnInvestedCapital DOUBLE, \n" +
            "    returnOnSales DOUBLE, \n" +
            "    shareBasedCompensation DOUBLE, \n" +
            "    sellingGeneralAndAdministrativeExpense DOUBLE, \n" +
            "    shareFactor DOUBLE, \n" +
            "    shares DOUBLE, \n" +
            "    weightedAverageShares DOUBLE, \n" +
            "    weightedAverageSharesDiluted DOUBLE, \n" +
            "    salesPerShare DOUBLE, \n" +
            "    tangibleAssetValue DOUBLE, \n" +
            "    taxAssets DOUBLE, \n" +
            "    incomeTaxExpense DOUBLE, \n" +
            "    taxLiabilities DOUBLE, \n" +
            "    tangibleAssetsBookValuePerShare DOUBLE, \n" +
            "    workingCapital DOUBLE \n" +
            ") TIMESTAMP(dateKey) PARTITION BY YEAR";
    public static void flushFinancials(String tableName, Stream<Financial> financials) {
        try (TableWriter writer = engine.getWriter(cairoSecurityContext, tableName)) {
            financials.sorted().forEach(financial -> {
                TableWriter.Row row = writer.newRow(financial.getDate());
                row.putSym(1, financial.ticker);
                row.putSym(2, financial.period);
                putTimestamp(row, 3, financial.calendarDate);
                putTimestamp(row, 4, financial.reportPeriod);
                putTimestamp(row, 5, financial.updated);
                putDouble(row, 6, financial.accumulatedOtherComprehensiveIncome);
                putDouble(row, 7, financial.assets);
                putDouble(row, 8, financial.assetsAverage);
                putDouble(row, 9, financial.assetsCurrent);
                putDouble(row, 10, financial.assetTurnover);
                putDouble(row, 11, financial.assetsNonCurrent);
                putDouble(row, 12, financial.bookValuePerShare);
                putDouble(row, 13, financial.capitalExpenditure);
                putDouble(row, 14, financial.cashAndEquivalents);
                putDouble(row, 15, financial.cashAndEquivalentsUSD);
                putDouble(row, 16, financial.costOfRevenue);
                putDouble(row, 17, financial.consolidatedIncome);
                putDouble(row, 18, financial.currentRatio);
                putDouble(row, 19, financial.debtToEquityRatio);
                putDouble(row, 20, financial.debt);
                putDouble(row, 21, financial.debtCurrent);
                putDouble(row, 22, financial.debtNonCurrent);
                putDouble(row, 23, financial.debtUSD);
                putDouble(row, 24, financial.deferredRevenue);
                putDouble(row, 25, financial.depreciationAmortizationAndAccretion);
                putDouble(row, 26, financial.deposits);
                putDouble(row, 27, financial.dividendYield);
                putDouble(row, 28, financial.dividendsPerBasicCommonShare);
                putDouble(row, 29, financial.earningBeforeInterestTaxes);
                putDouble(row, 30, financial.earningsBeforeInterestTaxesDepreciationAmortization);
                putDouble(row, 31, financial.EBITDAMargin);
                putDouble(row, 32, financial.earningsBeforeInterestTaxesDepreciationAmortizationUSD);
                putDouble(row, 33, financial.earningBeforeInterestTaxesUSD);
                putDouble(row, 34, financial.earningsBeforeTax);
                putDouble(row, 35, financial.earningsPerBasicShare);
                putDouble(row, 36, financial.earningsPerDilutedShare);
                putDouble(row, 37, financial.earningsPerBasicShareUSD);
                putDouble(row, 38, financial.shareholdersEquity);
                putDouble(row, 39, financial.averageEquity);
                putDouble(row, 40, financial.shareholdersEquityUSD);
                putDouble(row, 41, financial.enterpriseValue);
                putDouble(row, 42, financial.enterpriseValueOverEBIT);
                putDouble(row, 43, financial.enterpriseValueOverEBITDA);
                putDouble(row, 44, financial.freeCashFlow);
                putDouble(row, 45, financial.freeCashFlowPerShare);
                putDouble(row, 46, financial.foreignCurrencyUSDExchangeRate);
                putDouble(row, 47, financial.grossProfit);
                putDouble(row, 48, financial.grossMargin);
                putDouble(row, 49, financial.goodwillAndIntangibleAssets);
                putDouble(row, 50, financial.interestExpense);
                putDouble(row, 51, financial.investedCapital);
                putDouble(row, 52, financial.investedCapitalAverage);
                putDouble(row, 53, financial.inventory);
                putDouble(row, 54, financial.investments);
                putDouble(row, 55, financial.investmentsCurrent);
                putDouble(row, 56, financial.investmentsNonCurrent);
                putDouble(row, 57, financial.totalLiabilities);
                putDouble(row, 58, financial.currentLiabilities);
                putDouble(row, 59, financial.liabilitiesNonCurrent);
                putDouble(row, 60, financial.marketCapitalization);
                putDouble(row, 61, financial.netCashFlow);
                putDouble(row, 62, financial.netCashFlowBusinessAcquisitionsDisposals);
                putDouble(row, 63, financial.issuanceEquityShares);
                putDouble(row, 64, financial.issuanceDebtSecurities);
                putDouble(row, 65, financial.paymentDividendsOtherCashDistributions);
                putDouble(row, 66, financial.netCashFlowFromFinancing);
                putDouble(row, 67, financial.netCashFlowFromInvesting);
                putDouble(row, 68, financial.netCashFlowInvestmentAcquisitionsDisposals);
                putDouble(row, 69, financial.netCashFlowFromOperations);
                putDouble(row, 70, financial.effectOfExchangeRateChangesOnCash);
                putDouble(row, 71, financial.netIncome);
                putDouble(row, 72, financial.netIncomeCommonStock);
                putDouble(row, 73, financial.netIncomeCommonStockUSD);
                putDouble(row, 74, financial.netLossIncomeFromDiscontinuedOperations);
                putDouble(row, 75, financial.netIncomeToNonControllingInterests);
                putDouble(row, 76, financial.profitMargin);
                putDouble(row, 77, financial.operatingExpenses);
                putDouble(row, 78, financial.operatingIncome);
                putDouble(row, 79, financial.tradeAndNonTradePayables);
                putDouble(row, 80, financial.payoutRatio);
                putDouble(row, 80, financial.priceToBookValue);
                putDouble(row, 81, financial.priceEarnings);
                putDouble(row, 82, financial.priceToEarningsRatio);
                putDouble(row, 83, financial.propertyPlantEquipmentNet);
                putDouble(row, 84, financial.preferredDividendsIncomeStatementImpact);
                putDouble(row, 85, financial.sharePriceAdjustedClose);
                putDouble(row, 86, financial.priceSales);
                putDouble(row, 87, financial.priceToSalesRatio);
                putDouble(row, 88, financial.tradeAndNonTradeReceivables);
                putDouble(row, 89, financial.accumulatedRetainedEarningsDeficit);
                putDouble(row, 90, financial.revenues);
                putDouble(row, 91, financial.revenuesUSD);
                putDouble(row, 92, financial.researchAndDevelopmentExpense);
                putDouble(row, 93, financial.returnOnAverageAssets);
                putDouble(row, 94, financial.returnOnAverageEquity);
                putDouble(row, 95, financial.returnOnInvestedCapital);
                putDouble(row, 96, financial.returnOnSales);
                putDouble(row, 97, financial.shareBasedCompensation);
                putDouble(row, 98, financial.sellingGeneralAndAdministrativeExpense);
                putDouble(row, 99, financial.shareFactor);
                putDouble(row, 100, financial.shares);
                putDouble(row, 101, financial.weightedAverageShares);
                putDouble(row, 102, financial.weightedAverageSharesDiluted);
                putDouble(row, 103, financial.salesPerShare);
                putDouble(row, 104, financial.tangibleAssetValue);
                putDouble(row, 105, financial.taxAssets);
                putDouble(row, 106, financial.incomeTaxExpense);
                putDouble(row, 107, financial.taxLiabilities);
                putDouble(row, 108, financial.tangibleAssetsBookValuePerShare);
                putDouble(row, 109, financial.workingCapital);
                row.append();
            });
            writer.commit();
        }
    }

    public static void close() {
        engine.close();
    }
}
