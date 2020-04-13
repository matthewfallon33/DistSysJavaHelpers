package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace;

import java.io.File;
import java.io.IOException;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.GWFReader;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.One2HistoryReader;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.LogReader;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.SWFReader;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.filters.Ignore;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random.SimpleRandomTraceGenerator;


public class FileBasedTraceProducerFactory {
	/**
	 * Ensures the correct parser loads the trace file based on file name
	 * extensions.
	 * 
	 * @param fileName
	 *            the name of the file to be parsed and loaded for jobs
	 * @param from
	 *            the first job to be included from the trace
	 * @param to
	 *            the last job to be included from the trace
	 * @param furtherjobs
	 *            <i>true</i> if it is allowed to look further in the trace
	 * @param maxProcs
	 *            Only used by producers which are capable to scale their jobs
	 *            in processor count
	 * @param jobType
	 *            the kind of jobs to instantiate
	 * @return The trace producer. <b>Warning:</b> If the trace producer kind
	 *         could not be determined then the function returns with
	 *         <i>null</i>.
	 * @throws SecurityException
	 *             if the jobType cannot be instantiated correctly
	 * @throws NoSuchMethodException
	 *             if the jobType cannot be instantiated correctly
	 * @throws IOException
	 *             file reading problem
	 * @throws TraceManagementException
	 *             if a random trace generation behaves unexpectedly
	 */
	public static GenericTraceProducer getProducerFromFile(String fileName, int from, int to, boolean furtherjobs,
			int maxProcs, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException, IOException, TraceManagementException {
		GenericTraceProducer producer = null;
		if (fileName.endsWith(".gwf")) {
			producer = new GWFReader(fileName, from, to, furtherjobs, jobType);
		} else if (fileName.endsWith(".swf")) {
			producer = new SWFReader(fileName, from, to, furtherjobs, jobType);
		} else if (fileName.endsWith(".srtg")) {
			SimpleRandomTraceGenerator srtg = SimpleRandomTraceGenerator.getInstanceFromFile(jobType, fileName);
			srtg.setMaxTotalProcs(maxProcs);
			if (from != 0) {
				srtg.setJobNum(from);
				srtg.getAllJobs();
			}
			srtg.setJobNum(to - from);
			producer = srtg;
		} else if (fileName.endsWith(".one2")) {
			producer = new One2HistoryReader(fileName, from, to, furtherjobs, jobType);
		} else if(fileName.endsWith(".log")){
			producer = new LogReader(fileName, from, to, furtherjobs, jobType);
		}else {
			return null;
		}
		File ignoreFile = new File(fileName + ".ign");
		if (ignoreFile.exists()) {
			producer = new TraceFilter(producer, new Ignore(ignoreFile));
		}
		return producer;
	}
}
