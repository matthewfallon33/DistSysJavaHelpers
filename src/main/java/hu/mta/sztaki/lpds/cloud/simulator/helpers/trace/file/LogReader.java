package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

public class LogReader extends TraceFileReaderFoundation {

	public LogReader(String fileName, int from, int to, boolean allowReadingFurther, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("Log Format", fileName, from, to, allowReadingFurther, jobType);
	}

	/**
	 * Checks line is of valid log format
	 * @return True if line is valid or false if not
	 * @param line
	 *            The current line passed from the log file
	 * @return true if line is of valid log file format false if not
	 */
	@Override
	protected boolean isTraceLine(String line) {

		String[] splitLine = line.split("\\s+");

		if (splitLine.length == 4) {
			// System.out.println("Line length 4!");
			if (!isDate(splitLine[0])) {
				System.out.println("First Line entry must be of valid time format");
				return false;
			}
			if (!isFloat(splitLine[1])) {
				System.out.println("Second Line entry must be a floating point number");
				return false;
			}
			String[] jobID = splitLine[2].split("-");
			if (jobID.length != 5) {
				System.out.println("JOBID not 5: " + jobID.length);
				return false;
			}
			if (!isExecutable(splitLine[3])) {
				System.out.println("Invalid Job Type!");
				return false;
			}

		} else {
			System.out.println("Line Length not four!");
			return false;
		}
		return true;
	}

	@Override
	protected void metaDataCollector(String line) {

	}
	
	/**
	 * Checks if given string is recognized executable
	 * @param str
	 * 			The string element to be checked
	 * @return true if recognized executable format false if not
	 */
	private boolean isExecutable(String str) {
		if (!str.toLowerCase().contains("url") && !str.toLowerCase().contains("default")
				&& !str.toLowerCase().contains("export")) {
			return false;
		}
		return true;
	}

	/**
	 * Checks if given string is of correct date format
	 * @param str
	 *            The string element to be checked
	 * @return true if valid date format false if not
	 */
	private boolean isDate(String str) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("S");
			sdf.parse(str);
		} catch (ParseException e) {
			return false;
		}
		return true;
	}

	/**
	 * Checks if given string is a floating point number
	 * 
	 * @param str
	 *            The string element to be checked
	 * @return true if of float format false if not
	 */
	private boolean isFloat(String str) {
		try {
			float f = Float.parseFloat(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

	
	/**
	 * Creates a job from a valid .log lines parameters
	 * 
	 * @param str
	 *            The string element to be checked
	 * @return new Job instance
	 */
	@Override
	protected Job createJobFromLine(String line)
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {

		final String[] frags = line.split("\\s+");
//		long arrTime = (Long.parseLong(frags[0])/1000);
		float duration = Float.parseFloat(frags[1]);
		// System.out.println(arrTime);
		// System.out.println("Arrival Time in Seconds: " + arrTime/1000);
//		 System.out.println("Job Duration: " + duration);
//		rounding job execution time
//		in order to convert the floating point duration to a long datatype it must be rounded
		long longDuration = Math.round(duration);
//		 System.out.println("Rounded Job Duration: " + longDuration);
		return jobCreator.newInstance(frags[2], Long.parseLong(frags[0]), 0, longDuration, 1, 0, 512, null, null,
				frags[3], null, 0);
	}

}
