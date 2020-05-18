package polygon;

import com.google.common.util.concurrent.RateLimiter;
import polygon.models.OHLCV;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import polygon.models.Trade;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class PolygonClient {
    static String baseUrl = "https://api.polygon.io/v2";
    static String apiKey = System.getenv("POLYGON_KEY");
    static Gson gson = new Gson(); // Thread safe
    final static Logger logger = LogManager.getLogger(PolygonClient.class);
    // Not ideal, but we start getting 500s
    final static RateLimiter rateLimiter = RateLimiter.create(200);
    final static long estOffsetMicros = 5 * 60 * 60 * 1000000L;
    final static long dayMicros = 24 * 60 * 60 * 1000000L;
    public final static int perPage = 50000;

    private static String doRequest(String url) {
        for (int i = 0; i < 15; i++) {
            try {
                rateLimiter.acquire();
                HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
                con.setRequestProperty("Accept", "application/json");
                con.setConnectTimeout(5 * 1000);
                con.setReadTimeout(8 * 1000);

                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                con.disconnect();

                if (con.getResponseCode() == 200) {
                    return content.toString();
                }
                Thread.sleep(Math.min((i + 1) * (i + 1) * 100, 10000));
            } catch (SocketTimeoutException e) {
                logger.debug("{} timeout", url);
            } catch (MalformedURLException|ProtocolException|InterruptedException|SocketException e) {
                logger.warn("{} {}", url, e);
            } catch (IOException e) {
                if (!e.getMessage().contains("500")) {
                    e.printStackTrace();
                }
            }
        }

        logger.error("Retries exceeded for request {}", url);
        System.exit(2);
        return null;
    }

    public static List<Trade> getTradesForSymbol(String day, String symbol) {
        List<Trade> trades = new ArrayList<>();

        long offset = 0;
        while(true) {
            String url = String.format("%s/ticks/stocks/trades/%s/%s?apiKey=%s&limit=%d&timestamp=%d",
                    baseUrl, symbol, day, apiKey, perPage, offset);
            String content = doRequest(url);
            try {
                TradeResponse r = gson.fromJson(content, TradeResponse.class);
                trades.addAll(r.results);
                if (r.results_count < perPage) {
                    // Last page
                    for (Trade t : r.results) {
                        // Polygon returns UDT nanoseconds for trade data. We save in EST microseconds
                        t.timeMicros /= 1000;
                        t.timeMicros -= estOffsetMicros;
                    }
                    return trades;
                }
                offset = trades.get(trades.size() - 1).timeMicros;
            }
            catch (JsonSyntaxException e) {
                // Happens about once every 5 months that we get incomplete response
                logger.warn(e);
            }
        }
    }

    public static List<OHLCV> getAggsForSymbol(Calendar from, Calendar to, String type, String symbol) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        while(true) {
            String url = String.format("%s/aggs/ticker/%s/range/1/%s/%s/%s?apiKey=%s",
                    baseUrl, symbol, type, sdf.format(from.getTime()), sdf.format(to.getTime()), apiKey);
            String content = doRequest(url);
            try {
                AggResponse r = gson.fromJson(content, AggResponse.class);
                if (r == null) {
                    logger.warn("null response from {}", url);
                    continue;
                }
                else if (r.results == null) {
                    return new ArrayList<>();
                }
                else if (!r.ticker.equals(symbol)) {
                    logger.warn("requested symbol {} but got ticker {}", symbol, r.ticker);
                }
                return normalizeOHLCV(type, r.results, r.ticker);
            } catch (JsonSyntaxException e) {
                // Occasionally Polygon will do something stupid like give us this for GGE: {
                //      "T":"X:BTCUSD",
                //      "v":516831.5165334968,
                //      "vw":17.1859,
                //      "o":7821.73,
                //      "c":17.251,
                //      "h":8988,
                //      "l":17.015,
                //      "t":1305518400000,
                //      "n":1
                // }
                int printUntil = content.contains(",{") ? content.indexOf(",{") : content.length();
                logger.warn("bad JSON from {}:\n {}\n{}", url, e, content.substring(0, printUntil));
            }
        }
    }

    public static List<Split> getSplitsForSymbol(String symbol) {
        while(true) {
            String url = String.format("%s/reference/splits/%s?apiKey=%s",
                    baseUrl, symbol, apiKey);
            String content = doRequest(url);
            try {
                SplitResponse r = gson.fromJson(content, SplitResponse.class);
                if (r == null) {
                    logger.warn("null response from {}", url);
                    continue;
                }
                else if (r.count != r.results.size()) {
                    logger.warn("response says count={} but got results.size()={}", r.count, r.results.size());
                }
                return r.results;
            } catch (JsonSyntaxException e) {
                int printUntil = content.contains(",{") ? content.indexOf(",{") : content.length();
                logger.warn("bad JSON from {}:\n {}\n{}", url, e, content.substring(0, printUntil));
            }
        }
    }

    public static List<Dividend> getDividendsForSymbol(String symbol) {
        while(true) {
            String url = String.format("%s/reference/dividends/%s?apiKey=%s",
                    baseUrl, symbol, apiKey);
            String content = doRequest(url);
            try {
                DividendResponse r = gson.fromJson(content, DividendResponse.class);
                if (r == null) {
                    logger.warn("null response from {}", url);
                    continue;
                }
                else if (r.count != r.results.size()) {
                    logger.warn("response says count={} but got results.size()={}", r.count, r.results.size());
                }
                return r.results;
            } catch (JsonSyntaxException e) {
                int printUntil = content.contains(",{") ? content.indexOf(",{") : content.length();
                logger.warn("bad JSON from {}:\n {}\n{}", url, e, content.substring(0, printUntil));
            }
        }
    }

    public static List<Financial> getFinancialsForSymbol(String symbol) {
        while(true) {
            String url = String.format("%s/reference/financials/%s?apiKey=%s",
                    baseUrl, symbol, apiKey);
            String content = doRequest(url);
            try {
                FinancialResponse r = gson.fromJson(content, FinancialResponse.class);
                if (r == null) {
                    logger.warn("null response from {}", url);
                    continue;
                }
                return r.results;
            } catch (JsonSyntaxException e) {
                int printUntil = content.contains(",{") ? content.indexOf(",{") : content.length();
                logger.warn("bad JSON from {}:\n {}\n{}", url, e, content.substring(0, printUntil));
            }
        }
    }

    public static List<Ticker> getTickers() {
        // 65536
        List<Ticker> tickers = new ArrayList<>(1 << 16);

        int perPage = 50;
        for (int page = 1;; page++) {
            String url = String.format("%s/reference/tickers?apiKey=%s&sort=ticker&market=stocks&perpage=%d&page=%d",
                    baseUrl, apiKey, perPage, page);
            logger.info("Downloading reference tickers {} / 35259+", (page - 1) * 50);
            String content = doRequest(url);
            TickerResponse r = gson.fromJson(content, TickerResponse.class);
            tickers.addAll(r.tickers);

            if (r.tickers.size() < perPage) // Last page
                return tickers;
        }
    }

    private static List<OHLCV> normalizeOHLCV(String type, List<OHLCV> aggs, String ticker) {
        for (OHLCV candle : aggs) {
            if (ticker != null) {
                candle.ticker = ticker;
            }
            // Polygon returns UDT milliseconds for agg data. We save in EST microseconds
            candle.timeMicros *= 1000;
            if (type.equals("day")) {
                // There are varying hours for agg1d data (4:00am - 5:00am UDT)
                // Round down to nearest day
                candle.timeMicros -= candle.timeMicros % dayMicros;
            } else if (type.equals("minute")) {
                candle.timeMicros -= estOffsetMicros;
            }
        }

        return aggs;
    }

    public static List<OHLCV> getAgg1d(Calendar day) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        while(true) {
            String url = String.format("%s/aggs/grouped/locale/US/market/STOCKS/%s?apiKey=%s",
                    baseUrl, sdf.format(day.getTime()), apiKey);
            String content = doRequest(url);
            try {
                AggResponse r = gson.fromJson(content, AggResponse.class);
                if (r == null || r.results == null) {
                    logger.warn("null response from {}:\n{}", url, content);
                    continue;
                }
                return normalizeOHLCV("day", r.results, null);
            } catch (JsonSyntaxException e) {
                logger.warn("bad JSON from {}:\n {}\n{}", url, e, content.substring(0, content.indexOf(",{")));
            }
        }
    }
}
