package org.adbcj.perf;

import static org.adbcj.perf.Configuration.*;

import org.adbcj.mysql.netty.Adbcj;

public class Test {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Adbcj.init();
		org.adbcj.mysql.mina.Adbcj.init();
		String host = "localhost";
		final String tinyQuery = "SELECT 1";
		final int count = 500000;
		AdbcjQueryExperiment experiment = new AdbcjQueryExperiment(MYSQL_ADBCJ_NETTY, host, tinyQuery, count);

		experiment.init();
		experiment.execute();
		experiment.cleanup();

		System.out.println("Done!");
	}

}
