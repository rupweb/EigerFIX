package eigerfix;

import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils 
{
    public static String now()
    {
        Date today = new Date();
        
        SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
        String date = DATE_FORMAT.format(today);
        
    	return date + " ";
    }		
    
    public static Calendar AddBusinessDays(Calendar date, int days) throws Exception
	{
		if (days < 0)
		{
    		throw new Exception("days cannot be negative");
		}

		if (days == 0) return date;

		if (date.get(Calendar.DAY_OF_WEEK) == 7) // Saturday
		{
    		date.add(Calendar.DAY_OF_MONTH, 2);
    		days -= 1;
		}
		else if (date.get(Calendar.DAY_OF_WEEK) == 1) // Sunday
		{
			date.add(Calendar.DAY_OF_MONTH, 1);
    		days -= 1;
		}

		date.add(Calendar.DAY_OF_MONTH, days / 5 * 7);
		int extraDays = days % 5;

		if ((int)Calendar.DAY_OF_WEEK + extraDays > 5)
		{
				extraDays += 2;
		}
		
		date.add(Calendar.DAY_OF_MONTH, extraDays);

		return date;
	}
}
