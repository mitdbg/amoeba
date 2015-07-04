package core.access.benchmark;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.access.spark.Config;
import core.adapt.opt.WorkloadAnalyser;
import core.index.Settings;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

public class RunWorkloadAnalyser {
	ConfUtils cfg;
	
	public RunWorkloadAnalyser(ConfUtils cfg) {
		this.cfg = cfg;
	}

	public void writeQueries(String queries) {
		String pathToQueries = cfg.getHDFS_HOMEDIR() + "/queries";
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		byte[] fileBytes = queries.getBytes();
		HDFSUtils.writeFile(fs, pathToQueries, Config.replication, fileBytes, 0, fileBytes.length, false);		
	}
	
	public void generateQueries() {
		int numQueries = 15;
	
		String toWrite = "";
		for (int i=1; i <= numQueries; i++) {
			// DISCOUNT - Random (0.02 - 0.09); DISCOUNT+-0.01 range
			Random r = new Random();
			double ddisc = r.nextFloat() * 0.07 + 0.02;
			float disc = (float) ddisc;
			Predicate p1 = new Predicate(6, TYPE.FLOAT, disc - 0.01, PREDTYPE.GT);
			Predicate p2 = new Predicate(6, TYPE.FLOAT, disc + 0.01, PREDTYPE.LEQ);
			FilterQuery q = new FilterQuery(new Predicate[]{p1, p2});
			toWrite += q.toString() + "\n";
		}

		writeQueries(toWrite);
	}
	
	// access shipdate in cyclic pattern, selectivity constant
	public void generateShipDateCyclic(){
		int numQueries = 100;
		String toWrite = "";
		for (int i=0; i < numQueries; i++) {
			int year = 1993 + i % 6;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);
			FilterQuery q = new FilterQuery(new Predicate[]{p1, p2});
			toWrite += q.toString() + "\n";
		}
		
		writeQueries(toWrite);
	}

	// access shipdate at random points, selectivity constant
	public void generateShipDateAdHoc(){
		// ORDERDATE uniformly distributed between STARTDATE and (ENDDATE - 151 days)
		// STARTDATE = 1992-01-01 CURRENTDATE = 1995-06-17 ENDDATE = 1998-12-31
		// -> between 1992-01-01 and 1998-08-02
		// SHIPDATE [1...121] days after ORDERDATE -> between 1992-01-02 and 1998-12-01, 2525 days
		String toWrite = "";
		int numQueries = 30;
		double selectivity = 0.05;
		int range = 2525;
		Calendar c = new GregorianCalendar();
		for (int i=1; i <= numQueries; i++) {
			int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
			c.set(1992, Calendar.JANUARY, 02);
			c.add(Calendar.DAY_OF_MONTH, startOffset);
			SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
			c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
			SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));

			Predicate p1 = new Predicate(10, TYPE.DATE, startDate, PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, endDate, PREDTYPE.LEQ);
			FilterQuery q = new FilterQuery(new Predicate[]{p1, p2});
			toWrite += q.toString() + "\n";
		}
		
		writeQueries(toWrite);
	}

	// access shipdate starting with high selectivity and decreasing exponentially
	// randomly choose start of interval, which remains the same throughout
	public void generateShipDateDrillDown(){
		String toWrite = "";
		int numQueries = 30;
		double selectivity = 0.1;
		int range = 2525;
		Calendar c = new GregorianCalendar();

		int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
		c.set(1992, Calendar.JANUARY, 02);
		c.add(Calendar.DAY_OF_MONTH, startOffset);
		SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
		c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));

		int reduction = (int) (range * selectivity * -1);
		for (int i=1; i <= numQueries; i++) {
			SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));

			Predicate p1 = new Predicate(10, TYPE.DATE, startDate, PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, endDate, PREDTYPE.LEQ);
			FilterQuery q = new FilterQuery(new Predicate[]{p1, p2});
			toWrite += q.toString() + "\n";

			reduction /= 2;
			c.add(Calendar.DAY_OF_MONTH, reduction);
		}
		
		writeQueries(toWrite);
	}

	public static int numNodes(RNode node) {
		if (node.bucket == null) {
			return 1 + numNodes(node.leftChild) + numNodes(node.rightChild);
		} else {
			return 1;
		}
	}
	
	public static void main(String[] args) {
		String propertyFile = Settings.cartilageConf;
		ConfUtils cfg = new ConfUtils(propertyFile);
		RunWorkloadAnalyser rwa = new RunWorkloadAnalyser(cfg);
		rwa.generateShipDateDrillDown();
		
		WorkloadAnalyser wa = new WorkloadAnalyser(cfg, 13, 1000);
		RobustTreeHs rt = wa.getOptTree();
		
		List<Float> qCosts = wa.getPerQueryCost(rt.getRoot());
		String costString = "";
		for (int i=0; i<qCosts.size(); i++) {
			costString += qCosts.get(i) + " ";
		}

		System.out.println("Num Candidates: " + wa.getNumCandidates());
		System.out.println("Num Nodes: " + numNodes(rt.getRoot()));
		System.out.println("Costs: " + costString);
		
		String index = new String(rt.marshall());
		System.out.println(index);
	}
}
