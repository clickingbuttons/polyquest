import java.util.Calendar;
import java.util.GregorianCalendar;

public class MarketCalendar {
    public static boolean isWeekend(Calendar date) {
        int day = date.get(Calendar.DAY_OF_WEEK);
        if (day == Calendar.SATURDAY || day == Calendar.SUNDAY) {
            return true;
        }

        return false;
    }

    public static Calendar getEaster(int year) {
        // Anonymous Gregorian algorithm
        // https://en.wikipedia.org/wiki/Computus
        // https://math.stackexchange.com/questions/896954/decoding-gauss-easter-algorithm
        int a = year % 19,
                b = year / 100,
                c = year % 100,
                d = b / 4,
                e = b % 4,
                g = (8 * b + 13) / 25,
                h = (19 * a + b - d - g + 15) % 30,
                j = c / 4,
                k = c % 4,
                m = (a + 11 * h) / 319,
                r = (2 * e + 2 * j - k - h + m + 32) % 7,
                n = (h - m + r + 90) / 25,
                p = (h - m + r + n + 19) % 32;

        return new GregorianCalendar(year, n - 1, p);
    }

    public static boolean isMarketOpen(Calendar date) {
        int day = date.get(Calendar.DAY_OF_WEEK);
        int week = date.get(Calendar.DAY_OF_WEEK_IN_MONTH);
        int month = date.get(Calendar.MONTH);
        int year = date.get(Calendar.YEAR);

        // Weekend
        if (isWeekend(date)) {
            return false;
        }

        // New year's
        Calendar newYearDay = new GregorianCalendar(year, Calendar.JANUARY, 1);
        while (isWeekend(newYearDay)) {
            newYearDay.add(Calendar.DATE, 1);
        }
        if (date.compareTo(newYearDay) == 0) {
            return false;
        }

        // MLK day
        if (day == Calendar.MONDAY && week == 3 && month == Calendar.JANUARY) {
            return false;
        }

        // Washington's Birthday
        if (day == Calendar.MONDAY && week == 3 && month == Calendar.FEBRUARY) {
            return false;
        }

        // Good Friday
        Calendar easter = getEaster(year);
        easter.add(Calendar.DATE, -2);
        if (date.compareTo(easter) == 0) {
            return false;
        }

        // Memorial Day
        // Different date pre-1970, don't have to worry about this for now
        Calendar memorialDay = new GregorianCalendar(year, Calendar.MAY, 31);
        while (memorialDay.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY) {
            memorialDay.add(Calendar.DATE, -1);
        }
        if (date.compareTo(memorialDay) == 0) {
            return false;
        }

        // Independence Day
        Calendar independenceDay = new GregorianCalendar(year, Calendar.JULY, 4);
        if (independenceDay.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
            independenceDay.add(Calendar.DATE, -1);
        }
        else if (independenceDay.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
            independenceDay.add(Calendar.DATE, 1);
        }
        if (date.compareTo(independenceDay) == 0) {
            return false;
        }

        // Labor Day
        if (day == Calendar.MONDAY && week == 1 && month == Calendar.SEPTEMBER) {
            return false;
        }

        // Thanksgiving
        if (day == Calendar.THURSDAY && week == 4 && month == Calendar.NOVEMBER) {
            return false;
        }

        // Christmas
        Calendar christmasDay = new GregorianCalendar(year, Calendar.DECEMBER, 25);
        if (christmasDay.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
            christmasDay.add(Calendar.DATE, -1);
        }
        else if (christmasDay.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
            christmasDay.add(Calendar.DATE, 1);
        }
        if (date.compareTo(christmasDay) == 0) {
            return false;
        }

        // Hurricane Sandy
        Calendar hurricaneSandy = new GregorianCalendar(2012, Calendar.OCTOBER, 29);
        if (date.compareTo(hurricaneSandy) == 0) {
            return false;
        }

        Calendar hurricaneSandy2 = new GregorianCalendar(2012, Calendar.OCTOBER, 30);
        if (date.compareTo(hurricaneSandy2) == 0) {
            return false;
        }

        // George H.W. Bush death
        Calendar deadAt94 = new GregorianCalendar(2018, Calendar.DECEMBER, 05);
        if (date.compareTo(deadAt94) == 0) {
            return false;
        }

        return true;
    }
}
