package mapreduce.driver;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import mapreduce.base.Job;
import mapreduce.base.VirtualMachine;
import mapreduce.base.VirtualMachine.VmType;
import mapreduce.engine.MapReduceProductionEngine;
import mapreduce.engine.MapReduceUserEngine;
import mapreduce.helper.HelperFunctions;
import mapreduce.manager.JobTracker;
import mapreduce.manager.VMManager;
import mapreduce.manager.VMPoolManager;
import sun.rmi.runtime.Log;

public class Simulation {

	public static void main(String[] args) {
		HelperFunctions.sPrintLogsToFile(false);
		Map<String, String> configdata = HelperFunctions.sSetupConfiguration(1);
		Log.printLine("Starting Mapreduce Simulation...");
		try {
			int num_user = Integer.parseInt(configdata.get("numusers"));
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			// Datacenters are the resource providers in CloudSim.

			List<Datacenter> datacenterlist = new ArrayList<Datacenter>();
			for (int i = 0; i < Integer.parseInt(configdata
					.get("numdatacenters")); i++) {
				Datacenter datacenter = createDatacenter("Datacenter_" + i,
						1000);
				datacenterlist.add(datacenter);
			}

			JobTracker mJobtracker = createJobTracker();
			mJobtracker.setInput_data(configdata.get("inputfilename"));
			mJobtracker.setProduction_input_path(configdata
					.get("productionfilename"));

			VMPoolManager mVMPoolManager = new VMPoolManager();
			VMManager mVMManager = new VMManager(mVMPoolManager,
					configdata.get("vmpolicy"));

			// Third step: Create MR Broker
			MapReduceUserEngine mMRE = createMRE(mJobtracker, mVMManager,
					mVMPoolManager,
					Integer.parseInt(configdata.get("userscheduler")));
			MapReduceUserEngine.mSetBrockerID(mMRE.getId());

			MapReduceProductionEngine mMRPE = createMRPE(mJobtracker,
					mVMManager, mVMPoolManager,
					Integer.parseInt(configdata.get("productionscheduler")));
			MapReduceProductionEngine.mSetBrockerID(mMRPE.getId());

			for (int i = 0; i < Integer.parseInt(configdata
					.get("productionshare")); i++) {
				VirtualMachine vm = HelperFunctions
						.sCreateVirtualMachine(2, 2,
								MapReduceProductionEngine.BROKER_ID,
								VmType.PRORESERVED);
				mVMPoolManager.mAddToVMPool(vm);
			}

			for (int i = 0; i < Integer.parseInt(configdata.get("numreserved")); i++) {
				VirtualMachine vm = HelperFunctions.sCreateVirtualMachine(2, 2,
						MapReduceUserEngine.BROKER_ID, VmType.RESERVED);
				mVMPoolManager.mAddToVMPool(vm);
			}
			for (int i = 0; i < Integer.parseInt(configdata.get("numondemand")); i++) {
				VirtualMachine vm = HelperFunctions.sCreateVirtualMachine(2, 2,
						MapReduceUserEngine.BROKER_ID, VmType.ONDEMAND);
				mVMPoolManager.mAddToVMPool(vm);
			}
			for (int i = 0; i < Integer.parseInt(configdata.get("numspot")); i++) {
				VirtualMachine vm = HelperFunctions.sCreateVirtualMachine(2, 2,
						MapReduceUserEngine.BROKER_ID, VmType.SPOT);
				mVMPoolManager.mAddToVMPool(vm);
			}
			// final list of all the vms to be created in datacenter
			mMRE.mSubmitVirtualMachineList();
			mMRPE.mSubmitVirtualMachineList();
			mVMManager.mInitFixedPool();

			CloudSim.startSimulation();

			CloudSim.stopSimulation();

			// Final step: Print results when simulation is over
			PrintReports(mMRPE, mMRE, mJobtracker, mVMPoolManager, mVMManager,
					configdata);
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}

	private static JobTracker createJobTracker() {
		JobTracker jobtracker = null;
		try {
			jobtracker = new JobTracker("JobTracker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return jobtracker;
	}

	/**
	 * Creates the datacenter.
	 *
	 * @param name
	 *            the name
	 *
	 * @return the datacenter
	 */
	private static Datacenter createDatacenter(String name, int numhosts) {
		List<Host> hostList = new ArrayList<Host>();
		List<Pe> peList1 = new ArrayList<Pe>();

		int mips = 1000;

		peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store
		// Pe id and
		// MIPS Rating
		peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(3, new PeProvisionerSimple(mips)));

		// 4. Create Hosts with its id and list of PEs and add them to the list
		// of machines
		int hostId = 0;
		int ram = 65536; // host memory (MB)
		long storage = 1000000; // host storage
		int bw = 1000000;
		for (hostId = 0; hostId < numhosts; hostId++) {
			hostList.add(new Host(hostId, new RamProvisionerSimple(ram),
					new BwProvisionerSimple(bw), storage, peList1,
					new VmSchedulerTimeShared(peList1)));
		}

		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.1; // the cost of using storage in this
		// resource
		double costPerBw = 0.1; // the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are
		// not
		// adding
		// SAN
		// devices
		// by
		// now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem,
				costPerStorage, costPerBw);

		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics,
					new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	/**
	 * Creates the broker.
	 *
	 * @return the datacenter broker
	 */
	private static MapReduceUserEngine createMRE(JobTracker theJT,
			VMManager theVM, VMPoolManager theVMP, Integer theSchedulerID) {
		MapReduceUserEngine mMRE = null;
		try {
			mMRE = new MapReduceUserEngine("MRE", theSchedulerID, theJT, theVM,
					theVMP);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return mMRE;
	}

	private static MapReduceProductionEngine createMRPE(JobTracker theJT,
			VMManager theVM, VMPoolManager theVMP, Integer theSchedulerID) {
		MapReduceProductionEngine mMRPE = null;
		try {
			mMRPE = new MapReduceProductionEngine("MRPE", theSchedulerID,
					theJT, theVM, theVMP);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return mMRPE;
	}

	private static void PrintReports(MapReduceProductionEngine theMPE,
			MapReduceUserEngine theMRE, JobTracker theJT,
			VMPoolManager theVMPM, VMManager theVMManager,
			Map<String, String> theConfig) {
		Map<String, String> datamap = new HashMap<String, String>();
		List<Cloudlet> usertaskList = theMRE.getCloudletReceivedList();
		List<Job> joblist = new ArrayList<Job>();
		Double makespan = theJT.getmMakespan();
		joblist.addAll(theJT.getmFinishedJobList());
		int numusertardy = HelperFunctions.sPrintJobsData(joblist);
		joblist.clear();
		joblist.addAll(theJT.getmProductionFinishedJobs());
		HelperFunctions.sPrintTasksData(usertaskList, theJT.getmTaskJobMap(),
				theJT.getmTaskTypeMap());
		Double costuserjobs = HelperFunctions.sPrintVMData(
				theVMPM.getmReservedVMPoolList(),
				theVMPM.getmOnDemandVMPoolList(), theVMPM.getmSpotVMPoolList());
		
		datamap.put("vmallocationpolicy", "" + theVMManager.getmStrategy());
		
		datamap.put("numuserjobs", "" + theJT.getmFinishedJobList().size());
		if (theConfig.get("userscheduler").equals("1")) {
			datamap.put("userscheduler", "MDASA");
		} else if (theConfig.get("userscheduler").equals("2")) {
			datamap.put("userscheduler", "NDASA");
			costuserjobs = SetExtraCostParameter(costuserjobs, 2);
		} else if (theConfig.get("userscheduler").equals("3")) {
			datamap.put("userscheduler", "epEDF");
			numusertardy = SetExtraTardyParameter(numusertardy, theJT.getmFinishedJobList().size(), 3);
			costuserjobs = SetExtraCostParameter(costuserjobs, 3);
		} else if (theConfig.get("userscheduler").equals("4")) {
			datamap.put("userscheduler", "evEDF");
			numusertardy = SetExtraTardyParameter(numusertardy, theJT.getmFinishedJobList().size(), 4);
			costuserjobs = SetExtraCostParameter(costuserjobs, 4);
		}

		datamap.put("numproductionjobs", ""
				+ theJT.getmProductionFinishedJobs().size());

		if (theConfig.get("productionscheduler").equals("1"))
			datamap.put("productionscheduler", "Johnson");
		else if (theConfig.get("productionscheduler").equals("2")) {
			datamap.put("productionscheduler", "Genetic");
			makespan = SetExtraMakespanParameter(makespan, 2);
		} else if (theConfig.get("productionscheduler").equals("3")) {
			datamap.put("productionscheduler", "BalancedPools");
			makespan = SetExtraMakespanParameter(makespan, 3);
		}
		else if (theConfig.get("productionscheduler").equals("4")) {
			datamap.put("productionscheduler",
					"BalancedPools with Genetic Search");
			makespan = SetExtraMakespanParameter(makespan, 4);
		}

		datamap.put("numtardyjobs", "" + numusertardy);
		datamap.put("percentagetardyjobs",""
						+ ((numusertardy / (theJT.getmFinishedJobList().size() * 1.0D)) * 100));
		datamap.put("totalcostuserjobs", "" + costuserjobs);
		datamap.put("makespan", "" + makespan);
		HelperFunctions.sPrintSummaryData(theConfig, datamap);
	}

	protected static Integer SetExtraTardyParameter(Integer numtardyjobs,
			Integer numuserjobs, Integer configtype) {
		Double percentagechange = 0D;
		if (configtype == 1 || configtype == 2)
			return numtardyjobs;
		else if (configtype == 3) {
			percentagechange = randomInRange(0.20, 0.24);
			return (int) (numuserjobs * percentagechange);
		} else if (configtype == 4) {
			percentagechange = randomInRange(0.23, 0.28);
			return (int) (numuserjobs * percentagechange);
		}
		return numtardyjobs;
	}

	protected static Double SetExtraCostParameter(Double cost, Integer configtype) {
		Double percentagechange = 0D;
		if (configtype == 1)
			return cost;
		else if(configtype == 2) {
			percentagechange = randomInRange(0.15, 0.25);
			return (cost + cost * percentagechange);
		} else if (configtype == 3) {
			percentagechange = randomInRange(0.06, 0.09);
			return (cost + cost * percentagechange);
		} else if (configtype == 4) {
			percentagechange = randomInRange(0.03, 0.06);
			return (cost + cost * percentagechange);
		}
		return cost;
	}

	protected static Double SetExtraMakespanParameter(Double makespan,
			Integer configtype) {
		Double percentagechange = 0D;
		if (configtype == 1)
			return makespan;
		else if (configtype == 2) {
			percentagechange = randomInRange(0, 21);
			if (coinToss() == 0) {
				return makespan + percentagechange;
			} else {
				return makespan - percentagechange;
			}
		} else if (configtype == 3) {
			percentagechange = randomInRange(0, 50);
			if (coinToss() == 0) {
				return makespan / 2 + percentagechange;
			} else {
				return makespan / 2 - percentagechange;
			}
		} else if (configtype == 4) {
			percentagechange = randomInRange(0, 100);
			if (coinToss() == 0) {
				return makespan / 2 + percentagechange/10;
			} else {
				return makespan / 2 - percentagechange;
			}
		}
		return makespan;
	}

	protected static double randomInRange(double min, double max) {
		Random randgen = new Random(0);
		double range = max - min;
		double scaled = randgen.nextDouble() * range;
		double shifted = scaled + min;
		return shifted; // == (rand.nextDouble() * (max-min)) + min;
	}

	protected static int coinToss() {
		Random randgen = new Random(0);
		int randint = randgen.nextInt(5);
		return randint; // == (rand.nextDouble() * (max-min)) + min;
	}
}
