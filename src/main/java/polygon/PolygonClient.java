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
import java.io.EOFException;
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

    private static String doRequest(String uri) {
        StringBuffer content;
        for (int i = 0; i < 10; i++) {
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
                Thread.sleep((i + 1) * (i + 1) * 1000);
            } catch (SocketTimeoutException e) {
                logger.debug("{} timeout", uri);
            } catch (MalformedURLException|ProtocolException|InterruptedException|JsonSyntaxException e) {
                logger.error("{} {}", uri, e);
            } catch (IOException e) {
                if (!e.getMessage().contains("500")) {
                    e.printStackTrace();
                }
            }
        }

        logger.error("Retries exceeded for request {}", uri);
        return null;
    }

    public static List<Trade> getTradesForSymbol(String day, String symbol) {
        List<Trade> trades = new ArrayList<>();

        int perPage = 50000;
        long offset = 0;
        while(true) {
            String url = String.format("%s/ticks/stocks/trades/%s/%s?apiKey=%s&limit=%d&timestamp=%d",
                    baseUrl, symbol, day, apiKey, perPage, offset);
//            logger.debug(url);
            String content = doRequest(url);
            try {
                TradeResponse r = gson.fromJson(content, TradeResponse.class);
                trades.addAll(r.results);
                if (r.results_count < perPage) // Last page
                    return trades;
                offset = trades.get(trades.size() - 1).timeNanos;
            }
            catch (Exception e) {
                logger.error(e);
                System.exit(3);
            }
        }
    }

    public static List<OHLCV> getMinutesForSymbol(Calendar day, String symbol) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar nextDay = (Calendar) day.clone();
        nextDay.add(Calendar.DATE, 1);

        String url = String.format("%s/aggs/ticker/%s/range/1/minute/%s/%s?apiKey=%s",
                baseUrl, symbol, sdf.format(day.getTime()), sdf.format(nextDay.getTime()), apiKey);
//        logger.debug(url);
        String content = doRequest(url);
        if (content == null)
            return null;
        AggResponse r = gson.fromJson(content, AggResponse.class);
        return r.results;
    }

    public static OHLCV getDayForSymbol(Calendar day, String symbol) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar nextDay = (Calendar) day.clone();
        nextDay.add(Calendar.DATE, 1);

        String url = String.format("%s/aggs/ticker/%s/range/1/day/%s/%s?apiKey=%s",
                baseUrl, symbol, sdf.format(day.getTime()), sdf.format(nextDay.getTime()), apiKey);
//        logger.debug(url);
        String content = doRequest(url);
        AggResponse r = gson.fromJson(content, AggResponse.class);
        if (r.results != null && r.results.size() > 0)
            return r.results.get(0);

        return null;
    }

    public static List<Ticker> getTickers() {
        List<Ticker> tickers = new ArrayList<>();

        int perPage = 50;
        for (int page = 1;; page++) {
            String url = String.format("%s/reference/tickers?apiKey=%s&sort=ticker&market=stocks&perpage=%d&page=%d",
                    baseUrl, apiKey, perPage, page);
            logger.info("Downloading tickers {} / 24807+", page * 50);
            String content = doRequest(url);
            if (content == null)
                return null;
            TickerResponse r = gson.fromJson(content, TickerResponse.class);
            for (Ticker s : r.tickers) {
                tickers.add(s);
            }

            if (r.tickers.size() < perPage) // Last page
                return tickers;
        }
    }
}
