package utlis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    static SimpleDateFormat simpleTimeFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static Date date;

    public static String dateutils(int days) throws ParseException {
        date = simpleDateFormat.parse("1899-12-30");
        Calendar now = Calendar.getInstance();
        now.setTime(date);
        now.add(now.DATE, days);
        date = now.getTime();
        return simpleDateFormat.format(date);
    }

    public static String dateTimeUtils(long timeStamp) throws ParseException {
        String time = simpleTimeFormat.format(new Date(Long.parseLong(String.valueOf(timeStamp))));      // 时间戳转换成时间
        return time;
    }

    /**
     * 获取每天的开始时间 00:00:00:00
     * @return
     */
    public static long getStartTime() {
//        Calendar dateStart = Calendar.getInstance();
//        dateStart.setTime(date);
//        dateStart.set(Calendar.HOUR_OF_DAY, 0);
//        dateStart.set(Calendar.MINUTE, 0);
//        dateStart.set(Calendar.SECOND, 0);
//        return dateStart.getTime();


        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long todayZero = calendar.getTimeInMillis();
        return todayZero;

    }

    /**
     * 获取每天的开始时间 23:59:59:999
     *
     * @return
     */
    public static long getEndTime() {
        Calendar dateEnd = Calendar.getInstance();
//        dateEnd.setTime(date);
        dateEnd.set(Calendar.HOUR_OF_DAY, 23);
        dateEnd.set(Calendar.MINUTE, 59);
        dateEnd.set(Calendar.SECOND, 59);
        long todayZero = dateEnd.getTimeInMillis();
        return todayZero;
    }

    /**
     * 获取每小时的开始时间
     *
     * @return
     */
    public static long getHourStartTime() {
        Calendar calendar = Calendar.getInstance();

        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 000);
        long toHourZero = calendar.getTimeInMillis();
        return toHourZero;

    }

    /**
     * 获取每小时的结束时间
     *
     * @return
     */
    public static long getHourEndTime() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);
        long todayZero  = calendar.getTimeInMillis();
        return todayZero;
    }

    public static void main(String[] args) throws ParseException {

        System.out.println(getHourStartTime());
        System.out.println(getHourEndTime());
//        String dateutils = DateUtils.dateutils(44746);
//        String dateutils = DateUtils.dateTimeUtils(1680969600000L);
//        System.out.println(dateutils);

    }
}
