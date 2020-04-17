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

    private static String doRequest(String uri) {
        StringBuffer content;
        for (int i = 0; i < 150; i++) {
            try {
                rateLimiter.acquire();
                URL url = new URL(uri);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestProperty("Accept", "application/json");
                con.setRequestMethod("GET");
                con.setDoOutput(true);
                con.setConnectTimeout(5 * 1000);
                con.setReadTimeout(8 * 1000);

                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                con.disconnect();

                int code = con.getResponseCode();
                if (code == 200)
                    return content.toString();
                Thread.sleep((i + 1) * (i + 1) * 100);
            } catch (SocketTimeoutException e) {
                logger.debug("{} timeout", uri);
            } catch (MalformedURLException|ProtocolException|InterruptedException e) {
                logger.error("{} {}", uri, e);
            } catch (IOException e) {
                if (!e.getMessage().contains("500")) {
                    e.printStackTrace();
                }
            }
        }

        logger.error("Retries exceeded for request {}", uri);
        System.exit(2);
        return null;
    }

    public static List<Trade> getTradesForSymbol(String day, String symbol) {
        List<Trade> trades = new ArrayList<>();

        long offset = 0;
        while(true) {
            String url = String.format("%s/ticks/stocks/trades/%s/%s?apiKey=%s&limit=%d&timestamp=%d",
                    baseUrl, symbol, day, apiKey, perPage, offset);
//            logger.debug(url);
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
                logger.error(e);
            }
        }
    }

    public static List<OHLCV> getAggsForSymbol(Calendar from, Calendar to, String type, String symbol) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String url = String.format("%s/aggs/ticker/%s/range/1/%s/%s/%s?apiKey=%s",
                baseUrl, symbol, type, sdf.format(from.getTime()), sdf.format(to.getTime()), apiKey);
//        logger.debug(url);
        String content = doRequest(url);
        if (content == null)
            return new ArrayList<>();
        AggResponse r = gson.fromJson(content, AggResponse.class);
        if (r.results == null) {
            return new ArrayList<>();
        }
        for (OHLCV candle : r.results) {
            // Polygon returns UDT milliseconds for agg data. We save in EST microseconds
            candle.timeMicros *= 1000;
            if (type.compareTo("day") == 0) {
                // There are varying hours for agg1d data (4:00am - 5:00am UDT)
                // Round down to nearest day
                candle.timeMicros -= candle.timeMicros % dayMicros;
            }
            else if (type.compareTo("minute") == 0) {
                candle.timeMicros -= estOffsetMicros;
            }
        }
        if (r.results.size() == perPage) {
            logger.error("Exceeded maximum size {} for request {}!", perPage, url);
            System.exit(1);
        }
        return r.results;
    }

    public static List<Ticker> getTickers() {
        List<Ticker> tickers = new ArrayList<>();

        int perPage = 50;
        for (int page = 1;; page++) {
            String url = String.format("%s/reference/tickers?apiKey=%s&sort=ticker&market=stocks&perpage=%d&page=%d",
                    baseUrl, apiKey, perPage, page);
            logger.info("Downloading tickers {} / 34502+", (page - 1) * 50);
            String content = doRequest(url);
            if (content == null)
                return null;
            TickerResponse r = gson.fromJson(content, TickerResponse.class);
            tickers.addAll(r.tickers);

            if (r.tickers.size() < perPage) // Last page
                return tickers;
        }
    }
}
