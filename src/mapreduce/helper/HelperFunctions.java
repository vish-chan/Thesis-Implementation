package mapreduce.helper;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.ConfigurationException;

import mapreduce.base.Job;
import mapreduce.base.Job.Priority;
import mapreduce.base.VirtualMachine;
import mapreduce.base.VirtualMachine.VmType;
import mapreduce.manager.JobTracker.TaskType;

import org.omg.CORBA.portable.OutputStream;

import sun.rmi.runtime.Log;

public class HelperFunctions {

	static int taskid = 0;
	static int jobid = 0;
	static int slotid = 0;
	static int vmid = 0;
	static int file_suffix = 0;
	static String config_path = "resources\\config.xml";
	static String vm_logs_path = "results\\vm_logs";
	static String job_logs_path = "results\\job_logs";
	static String task_logs_path = "results\\task_logs";
	static String summary_logs_path = "results\\summary";
	public static String all_logs_path = "results\\all_logs";
	public static final int BLOCK_SIZE = 65536;

	public static String sFormatFilename(String filename) {
		filename = filename + "_" + sGetCurrentTimeStamp();
		return filename;
	}

	public static String sGetCurrentTimeStamp() {
		DateFormat sdfDate = new SimpleDateFormat("yyMMdd-HHmmss");// dd/MM/yyyy
		Calendar cal = Calendar.getInstance();
		String strDate = sdfDate.format(cal.getTime());
		return strDate;
	}

	public static Cloudlet sCreateTask(int uid, long tasklength) {
		int id = taskid++;
		long length = tasklength;
		long fileSize = 300;
		long outputSize = 300;
		int pesNumber = 1;
		UtilizationModel utilizationModel = new UtilizationModelFull();
		Cloudlet task = new Cloudlet(id, length, pesNumber, fileSize,
				outputSize, utilizationModel, utilizationModel,
				utilizationModel);
		task.setUserId(uid);
		return task;
	}

	public static Job sCreateJob(String name, int uid, Priority priority,
			long nummaptasks, long numreducetasks, Double avgmt, Double avgrt,
			Double minmt, Double maxmt, Double deadline, Double submittime) {
		Job job = new Job(name, uid, jobid++, priority, avgmt, avgrt, deadline,
				submittime);
		List<Cloudlet> maptasks = new ArrayList<Cloudlet>();
		List<Cloudlet> reducetasks = new ArrayList<Cloudlet>();
		for (int i = 0; i < nummaptasks; i++) {
			Cloudlet task = sCreateTask(uid,
					Math.round(minmt)
							+ (long) (Math.random() * ((maxmt - minmt) + 1)));
			maptasks.add(task);
		}
		job.mSetMapTasks(maptasks);
		for (int i = 0; i < numreducetasks; i++) {
			Cloudlet task = sCreateTask(uid, Math.round(avgrt));
			reducetasks.add(task);
		}
		job.mSetReduceTasks(reducetasks);
		return job;
	}

	public static Vm sCreateSlot(int uid) {
		// VM description
		int vmid = slotid++;
		int mips = 1;
		long size = 10000; // image size (MB)
		int ram = 512; // vm memory (MB)
		long bw = 1000;
		int pesNumber = 1; // number of cpus
		String vmm = "Xen"; // VMM name
		// create VM
		Vm slot = new Vm(vmid, uid, mips, pesNumber, ram, bw, size, vmm,
				new CloudletSchedulerSpaceShared());
		return slot;

	}

	public static VirtualMachine sCreateVirtualMachine(int nummapslots,
			int numreduceslots, int uid, VmType type) {
		VirtualMachine vm = new VirtualMachine(uid, vmid++, type);
		List<Vm> mapslots = new ArrayList<Vm>();
		List<Vm> reduceslots = new ArrayList<Vm>();
		for (int i = 0; i < nummapslots; i++) {
			Vm slot = sCreateSlot(uid);
			mapslots.add(slot);
		}
		vm.mSetMSList(mapslots);
		for (int i = 0; i < numreduceslots; i++) {
			Vm slot = sCreateSlot(uid);
			reduceslots.add(slot);
		}
		vm.mSetRSList(reduceslots);
		return vm;
	}

	public static void mSetOutputStream(String filename) {
		try {
			FileOutputStream out = new FileOutputStream(filename);
			Log.setOutput(out);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void mSetOutputStream(OutputStream sname) {
		Log.setOutput(sname);
	}
	
	public static void sPrintLogsToFile(boolean b) {
		if(b==false)
			return;
		String formattedfilename = sFormatFilename(all_logs_path + "_config-"
				+ file_suffix)
				+ ".txt";
		HelperFunctions.mSetOutputStream(formattedfilename);	
	}

	public static int sPrintJobsData(List<Job> joblist) {
		String formattedfilename = sFormatFilename(job_logs_path + "_config-"
				+ file_suffix)
				+ ".txt";
		HelperFunctions.mSetOutputStream(formattedfilename);
		int size = joblist.size();
		Job job;
		int numberoftardyjobs = 0;
		NumberFormat formatter = new DecimalFormat("0000000");
		String indent = "\t";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Job ID" + indent + "STATUS" + indent + "Submit_Time"
				+ indent + "Start_Time" + indent + "Finish Time" + indent
				+ "Expected_FT" + indent + "Lateness" + indent + "is_Tardy");

		DecimalFormat dft = new DecimalFormat("000000.00");
		for (int i = 0; i < size; i++) {
			job = joblist.get(i);
			Log.print(job.getmName() + indent);
			Log.print("SUCCESS");
			Log.printLine(indent+formatter.format(job.getmSubmitTime())
					+ indent
					+ formatter.format(job.getmStartTime())
					+ indent
					+ (dft.format(job.getmFinishTime()))
					+ indent
					+ (dft.format(job.getmSubmitTime() + job.getmDeadline()))
					+ indent
					+ (dft.format(job.getmExpectedFinishTime()
							- job.getmFinishTime()))
					+ indent
					+ (job.getmExpectedFinishTime()
							- job.getmFinishTime() >= 0 ? false : true)
					+ indent + job.getmNumMapTasks() + "|"
					+ job.getmNumReduceTasks() + indent + job.getmNumMapSlots()
					+ "|" + job.getmNumReduceSlots());
			if((job.getmExpectedFinishTime() - job.getmFinishTime()) < 0)
				numberoftardyjobs++;
		}
		return numberoftardyjobs;
	}

	public static void sPrintTasksData(List<Cloudlet> list,
			HashMap<Integer, Long> taskjobmap,
			HashMap<Integer, TaskType> mTaskTypeMap) {
		String formattedfilename = sFormatFilename(task_logs_path + "_config-"
				+ file_suffix)
				+ ".txt";
		HelperFunctions.mSetOutputStream(formattedfilename);

		int size = list.size();
		Cloudlet cloudlet;
		String indent = "\t";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("TaskID" + indent + "JobID" + indent + "STATUS" + indent
				+ "DatacenterID" + indent + "SlotID"+ indent + "Time"
				+ indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(cloudlet.getCloudletId()+ indent
					+ taskjobmap.get(cloudlet.getCloudletId()) + indent + mTaskTypeMap.get(cloudlet.getCloudletId())
					+ indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print("SUCCESS");

				Log.printLine(indent + cloudlet.getResourceId()
						+ indent 
						+ cloudlet.getVmId()
						+ indent
						+ dft.format(cloudlet.getActualCPUTime())
						+ indent 
						+ dft.format(cloudlet.getExecStartTime())
						+ indent
						+ dft.format(cloudlet.getFinishTime()));
			}
		}
	}

	public static Double sPrintVMData(List<VirtualMachine> reservedvmlist,
			List<VirtualMachine> ondemandvmlist, List<VirtualMachine> spotvmlist) {
		String formattedfilename = sFormatFilename(vm_logs_path + "_config-"
				+ file_suffix)
				+ ".txt";
		HelperFunctions.mSetOutputStream(formattedfilename);
		DecimalFormat dft = new DecimalFormat("###.##");
		String indent = "\t";
		Double totalcost = 0.0D;
		List<VirtualMachine> vmlist = new ArrayList<VirtualMachine>();
		vmlist.addAll(reservedvmlist);
		Log.printLine();
		Log.printLine("========== Fixed Pool ==========");
		Log.printLine("VM ID" + indent + "TYPE" + indent
				+ "Price($/year)" + indent + "Total Allocation Time"
				+ indent + "Total Incurred Cost($/year)");
		for (VirtualMachine vm : vmlist) {
			totalcost += vm.getmIncurredCost();
			Log.printLine(vm.__getId()+ indent + vm.getmType()
					+ indent 
					+ dft.format(vm.getmPrice()) 
					+ indent
					+ dft.format(vm.getmTotalAllocatedTime())
					+ indent
					+ dft.format(vm.getmIncurredCost()));
		}
		Log.printLine("Total cost for fixed pool in $ per year: " + totalcost);
		totalcost = 0D;
		vmlist.clear();
		vmlist.addAll(ondemandvmlist);
		vmlist.addAll(spotvmlist);
		Log.printLine();
		Log.printLine("========== Floating Pool ==========");
		Log.printLine("VM ID" + indent + "TYPE" + indent
				+ "Price($/hour)"+ indent + "Total Allocation Time"
				+ indent + "Total Incurred Cost($)");
		for (VirtualMachine vm : vmlist) {
			if (vm.getmTotalAllocatedTime() > 0) {
				totalcost += vm.getmIncurredCost();
				Log.printLine(vm.__getId() 
						+ indent 
						+ vm.getmType()
						+ indent 
						+ dft.format(vm.getmPrice())
						+ indent
						+ dft.format(vm.getmTotalAllocatedTime()) 
						+ indent
						+ dft.format(vm.getmIncurredCost()));
			}
		}
		return totalcost;
	}
	
	public static void sPrintSummaryData(Map<String, String> configuration, Map<String, String> data) {
		String formattedfilename = sFormatFilename(summary_logs_path + "_config-"
				+ file_suffix)
				+ ".txt";
		HelperFunctions.mSetOutputStream(formattedfilename);
		Log.printLine("Configuration number: " + file_suffix);
		Log.printLine("VM allocation policy: " + data.get("vmallocationpolicy"));
		Log.printLine("Number of user jobs:" + data.get("numuserjobs"));
		Log.printLine("User scheduler: " + data.get("userscheduler"));
		Log.printLine("Number of production jobs:" + data.get("numproductionjobs"));
		Log.printLine("Production Scheduler: "+data.get("productionscheduler"));
		Log.printLine("Total number of tardy jobs: "+data.get("numtardyjobs"));
		Log.printLine("Percentage of tardy jobs: "+data.get("percentagetardyjobs"));
		Log.printLine("Total cost of Vms for user jobs in $: " + data.get("totalcostuserjobs"));
		Log.printLine("Number of production Vms: "+configuration.get("productionshare"));
		Log.printLine("Makespan for production jobs in seconds: " + data.get("makespan"));
	}

	public static Map<String, String> sSetupConfiguration(int i) {
		file_suffix = i;
		Map<String, String> configdata = new HashMap<String, String>();
		String basestring = "configurations.configuration(" + i + ").";
		XMLConfiguration xmlconf = null;
		try {
			xmlconf = new XMLConfiguration(config_path);
			configdata.put("numusers",
					xmlconf.getString(basestring + "numusers"));
			configdata.put("numdatacenters",
					xmlconf.getString(basestring + "numdatacenters"));
			configdata.put("numreserved",
					xmlconf.getString(basestring + "numreserved"));
			configdata.put("numondemand",
					xmlconf.getString(basestring + "numondemand"));
			configdata
					.put("numspot", xmlconf.getString(basestring + "numspot"));
			configdata.put("inputfilename",
					xmlconf.getString(basestring + "inputfilename"));
			configdata
					.put("userscheduler", xmlconf.getString("User.Scheduler"));
			configdata.put("vmpolicy",
					xmlconf.getString("User.VmAllocationPolicy"));
			configdata.put("productionscheduler",
					xmlconf.getString("Production.Scheduler"));
			configdata.put("productionshare",
					xmlconf.getString("Production.Share"));
			configdata.put("productionfilename",
					xmlconf.getString("Production.Filename"));
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		return configdata;
	}
}
