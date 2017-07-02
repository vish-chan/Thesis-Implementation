package mapreduce.engine;

import java.util.ArrayList;
import java.util.List;

import mapreduce.base.Job;
import mapreduce.base.VirtualMachine;
import mapreduce.base.VirtualMachine.SlotType;
import mapreduce.manager.JobTracker;
import mapreduce.manager.VMManager;
import mapreduce.manager.VMPoolManager;
import mapreduce.scheduler.MASwithGeneticAlgorithm;
import mapreduce.scheduler.MASwithJohnsonAlgorithm;
import sun.rmi.runtime.Log;

public class MapReduceProductionEngine extends DatacenterBroker {
	public static int BROKER_ID = 1;
	int mSchedulerID;
	List<Job> mJobBatch;
	JobTracker mJobTracker;
	VMManager mVMManager;
	VMPoolManager mVMPoolManager;
	MASwithJohnsonAlgorithm mMASwithJohnsonAlgorithm;
	MASwithGeneticAlgorithm mMASwithGeneticAlgorithm;

	public MapReduceProductionEngine(String name, int theSchedulerID,
			JobTracker theJT, VMManager theVM, VMPoolManager theVMPOOL)
					throws Exception {
		super(name);
		mJobBatch = new ArrayList<Job>();
		mSchedulerID = theSchedulerID;
		mJobTracker = theJT;
		mVMManager = theVM;
		mVMPoolManager = theVMPOOL;
		mMASwithJohnsonAlgorithm = new MASwithJohnsonAlgorithm(mVMManager,
				mJobTracker);
		mMASwithGeneticAlgorithm = new MASwithGeneticAlgorithm(mVMManager,
				mJobTracker);
	}



	@Override
	public void startEntity() {
		super.startEntity();
	}

	// a wrapper for submitVmList
	public void mSubmitVirtualMachineList() {
		List<Vm> slotlist = new ArrayList<Vm>();
		List<VirtualMachine> vmlist = new ArrayList<VirtualMachine>();
		vmlist.addAll(mVMPoolManager.getmProductionVMPoolList());
		for (VirtualMachine vm : vmlist) {
			for (Vm slot : vm.__getMSList()) {
				slotlist.add(slot);
			}
			for (Vm slot : vm.__getRSList()) {
				slotlist.add(slot);
			}
		}
		this.submitVmList(slotlist);
	}


	public void mSubmitTasksToDatacenter(Job job, int type) {
		this.mSubmitCloudletList(job, type);
		submitCloudlets();
	}

	/**
	 * @param job
	 * @param type
	 */
	public void mSubmitCloudletList(Job job, int type) {
		List<Cloudlet> tasklist = new ArrayList<Cloudlet>();
		if (type == 1) {
			for (Cloudlet mtask : job.__getMTList())
				tasklist.add(mtask);
			Log.printLine("Submitting map tasks");
		} else {
			for (Cloudlet rtask : job.__getRTList())
				tasklist.add(rtask);
			Log.printLine("Submitting reduce tasks");
		}
		this.submitCloudletList(tasklist);
	}

	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		case CloudSimTags.CLOUDLET_RETURN:
			processCloudletReturn(ev);
			break;
		case CloudSimTags.NEW_BATCH:
			processNewBatch(ev);
			break;
		case CloudSimTags.LAUNCH_REDUCE:
			processLaunchReduce(ev);
			break;
		case CloudSimTags.SHUTDOWN:
			clearDatacenters();
			finishExecution();
			break;
		default:
			super.processEvent(ev);
			break;
		}
	}

	protected void createVmInDatacenter(int datacenterId) {
		int requestedVms = 0;
		String datacenterName = CloudSim.getEntityName(datacenterId);
		for (Vm vm : getVmList()) {
			if (!getVmsToDatacentersMap().containsKey(vm.getId())) {
				Log.printLine(CloudSim.clock() + ": " + getName()
						+ ": Trying to Create VM #" + vm.getId() + " in "
						+ datacenterName);
				sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
				requestedVms++;
			}
		}

		getDatacenterRequestedIdsList().add(datacenterId);
		setVmsRequested(requestedVms);
		setVmsAcks(0);
	}

	@SuppressWarnings("unchecked")
	protected void processNewBatch(SimEvent ev) {
		List<Job> joblist = (List<Job>) ev.getData();
		Job newJob = null, firstJob = null;
		if (mSchedulerID == 1) {
			mJobBatch = mMASwithJohnsonAlgorithm.ScheduleJobBatch(joblist);
			firstJob = mJobBatch.get(0);
			newJob = mMASwithJohnsonAlgorithm.ScheduleJob(firstJob, 1);
		}
		else {
			mJobBatch = mMASwithGeneticAlgorithm.ScheduleJobBatch(joblist);
			firstJob = mJobBatch.get(0);
			newJob = mMASwithGeneticAlgorithm.ScheduleJob(firstJob, 1);
		}
		mSubmitTasksToDatacenter(newJob, 1);
		schedule(mJobTracker.getId(), 0, CloudSimTags.RUNNING_JOB, newJob);
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		int slotid = cloudlet.getVmId();
		int taskid = cloudlet.getCloudletId();
		SlotType slottype = mVMManager.getmSlotType(slotid);
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Task " + taskid
				+ " received");
		if (mSchedulerID == 1) {
			mMASwithJohnsonAlgorithm.mRemoveEntryFromTaskSlotMap(taskid);
			if (mMASwithJohnsonAlgorithm.mIsSlotIdle(slotid)) {
				if (slottype == SlotType.MAP) {
					mMASwithJohnsonAlgorithm
					.mRemoveEntryFromMapSlotOccupanceMap(slotid);
				} else {
					mMASwithJohnsonAlgorithm
					.mRemoveEntryFromReduceSlotOccupanceMap(slotid);
				}
				Log.printLine(CloudSim.clock() + ": Slot #" + slotid
						+ " sent to idle list");
			}
		} else {
			mMASwithGeneticAlgorithm.mRemoveEntryFromTaskSlotMap(taskid);
			if (mMASwithGeneticAlgorithm.mIsSlotIdle(slotid)) {
				if (slottype == SlotType.MAP) {
					mMASwithGeneticAlgorithm
					.mRemoveEntryFromMapSlotOccupanceMap(slotid);
				} else {
					mMASwithGeneticAlgorithm
					.mRemoveEntryFromReduceSlotOccupanceMap(slotid);
				}
				Log.printLine(CloudSim.clock() + ": Slot #" + slotid
						+ " sent to idle list");
			}
		}
		mVMManager.mProduction_ChangeScheduledTaskCount(slotid, -1);
		schedule(mJobTracker.getId(), 0, CloudSimTags.TASK_COMPLETION, taskid);
		getCloudletReceivedList().add(cloudlet);
		cloudletsSubmitted--;
	}

	public void processLaunchReduce(SimEvent ev) {
		Job curjob = null, nextjob = null;
		Long jobid = new Long((long) ev.getData());
		int idx = 0;
		for (Job job : mJobBatch) {
			if (job.__getId() == jobid) {
				idx = mJobBatch.indexOf(job)+1;
				if (mSchedulerID == 1) {
					curjob = mMASwithJohnsonAlgorithm.ScheduleJob(job, 2);
					if(idx<mJobBatch.size()) {
						nextjob = mMASwithJohnsonAlgorithm.ScheduleJob(mJobBatch.get(idx), 1);
					}
				}
				else {
					curjob = mMASwithGeneticAlgorithm.ScheduleJob(job, 2);
					if(idx<mJobBatch.size()) {
						nextjob = mMASwithGeneticAlgorithm.ScheduleJob(mJobBatch.get(idx), 1);
					}
				}
				mSubmitTasksToDatacenter(curjob, 2);
				if(nextjob!=null) {
					mSubmitTasksToDatacenter(nextjob, 1);
					schedule(mJobTracker.getId(), 0, CloudSimTags.RUNNING_JOB, nextjob);
				}
				break;
			}
		}
	}

	@Override
	protected void submitCloudlets() {
		int vmIndex = 0;
		for (Cloudlet cloudlet : getCloudletList()) {
			Vm vm;
			if (cloudlet.getVmId() == -1) {
				vm = getVmsCreatedList().get(vmIndex);
			} else { // submit to the specific vm
				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
				if (vm == null) { // vm was not created
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": Postponing execution of cloudlet "
							+ cloudlet.getCloudletId()
							+ ": bount VM not available");
					continue;
				}
			}

			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": Sending task " + cloudlet.getCloudletId()
					+ " to slot #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()),
					CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
			getCloudletSubmittedList().add(cloudlet);
		}
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}

	public static void mSetBrockerID(int id) {
		BROKER_ID = id;
	}

	/**
	 * @return the bROKER_ID
	 */
	public static int getBROKER_ID() {
		return BROKER_ID;
	}

	/**
	 * @param bROKER_ID
	 *            the bROKER_ID to set
	 */
	public static void setBROKER_ID(int bROKER_ID) {
		BROKER_ID = bROKER_ID;
	}

	/**
	 * @return the mJobtracker
	 */
	public JobTracker getmJobtracker() {
		return mJobTracker;
	}

	/**
	 * @param mJobtracker
	 *            the mJobtracker to set
	 */
	public void setmJobtracker(JobTracker mJobtracker) {
		this.mJobTracker = mJobtracker;
	}

	/**
	 * @return the mVMManager
	 */
	public VMManager getmVMManager() {
		return mVMManager;
	}

	/**
	 * @param mVMManager
	 *            the mVMManager to set
	 */
	public void setmVMManager(VMManager mVMManager) {
		this.mVMManager = mVMManager;
	}

	/**
	 * @return the mVMPoolManager
	 */
	public VMPoolManager getmVMPoolManager() {
		return mVMPoolManager;
	}

	/**
	 * @param mVMPoolManager
	 *            the mVMPoolManager to set
	 */
	public void setmVMPoolManager(VMPoolManager mVMPoolManager) {
		this.mVMPoolManager = mVMPoolManager;
	}
}
