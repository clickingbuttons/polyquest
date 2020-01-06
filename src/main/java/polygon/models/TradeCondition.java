package polygon.models;

// 2019-01-24
// https://api.polygon.io/v1/meta/conditions/trades?apiKey=abcde
public enum TradeCondition {
    Regular (0),
    Acquisition (1),
    AveragePrice (2),
    AutomaticExecution (3),
    Bunched (4),
    BunchSold (5),
    CAPElection (6),
    CashTrade (7),
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
    RegularStoppedStock (27),
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
    Errored (54)
    ;

    public final int condition;

    TradeCondition(int condition) {
        this.condition = condition;
    }
}
