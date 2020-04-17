package polygon.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum TradeCondition {
    Regular (0),
    Acquisition (1),
    AveragePrice (2),
    AutomaticExecution (3),
    Bunched (4),
    BunchedSold(5),
    CAPElection (6),
    Cash(7),
    Closing (8),
    Cross (9),
    DerivativelyPriced (10),
    Distribution (11),
    FormTExtendedHours (12),
    FormTOutOfSequence (13),
    InterMarketSweep (14),
    MarketCenterOfficialClose (15),
    MarketCenterOfficialOpen (16),
    MarketCenterOpening (17),
    MarketCenterReOpening(18),
    MarketCenterClosing (19),
    NextDay (20),
    PriceVariation (21),
    PriorReferencePrice (22),
    Rule155Amex (23),
    Rule127Nyse (24),
    Opening (25),
    Opened (26),
    Stopped (27),
    ReOpening (28),
    Seller (29),
    SoldLast (30),
    SoldLastStoppedStock (31),
    SoldOutOfSequence (32),
    SoldOutOfSequenceStoppedStock (33),
    Split (34),
    StockOption (35),
    YellowFlag (36),
    OddLot (37),
    CorrectedConsolidatedClosePrice (38),
    Unknown (39),
    Held (40),
    TradeThruExempt (41),
    NonEligible (42),
    NonEligibleExtended (43),
    Cancelled (44),
    Recovery (45),
    Correction (46),
    AsOf (47),
    AsOfCorrection (48),
    AsOfCancel (49),
    OOB (50),
    Summary (51),
    Contingent (52),
    ContingentQualified (53),
    Errored (54),
    OpeningReopening (55),
    Placeholder (56),
    Placeholder611Exempt (57)
    ;

    // Follow consolidated pricing guidelines
    // https://www.ctaplan.com/publicdocs/ctaplan/notifications/trader-update/CTS_BINARY_OUTPUT_SPECIFICATION.pdf
    // PAGE 61
    // https://polygon.io/glossary/us/stocks/conditions-indicators
    // https://polygon.io/glossary/us/stocks/trade-conditions
    public static List<TradeCondition> eligibleHighLow = Arrays.asList(new TradeCondition[]{
            TradeCondition.Regular,
            TradeCondition.Acquisition,
            TradeCondition.AutomaticExecution,
            TradeCondition.Bunched,
            TradeCondition.BunchedSold,
            TradeCondition.Closing,
            TradeCondition.Cross,
            TradeCondition.DerivativelyPriced,
            TradeCondition.Distribution,
            TradeCondition.InterMarketSweep,
            TradeCondition.PriorReferencePrice,
            TradeCondition.Rule155Amex,
            TradeCondition.Opening,
            TradeCondition.Stopped,
            TradeCondition.ReOpening,
            TradeCondition.Seller,
            TradeCondition.SoldLast,
            TradeCondition.SoldOutOfSequence,
            TradeCondition.Split,
            TradeCondition.YellowFlag,
            TradeCondition.CorrectedConsolidatedClosePrice
    });
    public static List<TradeCondition> eligibleLast = Arrays.asList(new TradeCondition[]{
            TradeCondition.Regular,
            TradeCondition.Acquisition,
            TradeCondition.AutomaticExecution,
            TradeCondition.Bunched,
            TradeCondition.Closing,
            TradeCondition.Cross,
            TradeCondition.Distribution,
            TradeCondition.InterMarketSweep,
            TradeCondition.Rule155Amex,
            TradeCondition.Opening,
            TradeCondition.Stopped,
            TradeCondition.ReOpening,
            TradeCondition.SoldLast,
            TradeCondition.Split,
            TradeCondition.YellowFlag,
            TradeCondition.CorrectedConsolidatedClosePrice
    });
    public static List<TradeCondition> eligibleVolume = Arrays.asList(new TradeCondition[]{
            TradeCondition.Regular,
            TradeCondition.Acquisition,
            TradeCondition.AveragePrice,
            TradeCondition.AutomaticExecution,
            TradeCondition.Bunched,
            TradeCondition.BunchedSold,
            TradeCondition.Cash,
            TradeCondition.Closing,
            TradeCondition.Cross,
            TradeCondition.DerivativelyPriced,
            TradeCondition.Distribution,
            TradeCondition.FormTExtendedHours,
            TradeCondition.FormTOutOfSequence,
            TradeCondition.InterMarketSweep,
            TradeCondition.NextDay,
            TradeCondition.PriceVariation,
            TradeCondition.PriorReferencePrice,
            TradeCondition.Rule155Amex,
            TradeCondition.Opening,
            TradeCondition.Stopped,
            TradeCondition.ReOpening,
            TradeCondition.Seller,
            TradeCondition.SoldLast,
            TradeCondition.SoldOutOfSequence,
            TradeCondition.Split,
            TradeCondition.YellowFlag,
            TradeCondition.OddLot,
            TradeCondition.Contingent,
            TradeCondition.ContingentQualified
    });


    public final int condition;

    TradeCondition(int condition) {
        this.condition = condition;
    }


}
