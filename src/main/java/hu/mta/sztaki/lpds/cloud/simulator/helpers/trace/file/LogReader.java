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

	@Override
	protected boolean isTraceLine(String line) {

		String[] splitLine = line.split("\\s+");

		if (line.split(" ").length == 4) {
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

	private boolean isExecutable(String str) {
		if (!str.toLowerCase().contains("url") && !str.toLowerCase().contains("default")
				&& !str.toLowerCase().contains("export")) {
			return false;
		}
		return true;
	}

	private boolean isDate(String str) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("S");
			sdf.parse(str);
		} catch (ParseException e) {
			return false;
		}
		return true;
	}

	private boolean isFloat(String str) {
		try {
			float f = Float.parseFloat(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

	@Override
	protected Job createJobFromLine(String line)
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {

		final String[] frags = line.split("\\s+");
		long arrTime = (Long.parseLong(frags[0])/1000);
		float duration = Float.parseFloat(frags[1]);
		// System.out.println(arrTime);
		// System.out.println("Arrival Time in Seconds: " + arrTime/1000);
		// System.out.println("Job Duration: " + duration);
		long longDuration = Math.round(duration);
		// System.out.println("Rounded Job Duration: " + longDuration);
		// Job job = new Job();
		return jobCreator.newInstance(frags[2], Long.parseLong(frags[0]), 0, longDuration, 1, 0, 512, null, null,
				frags[3], null, 0);
	}

}
