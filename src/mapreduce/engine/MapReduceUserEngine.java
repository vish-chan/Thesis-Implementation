package mapreduce.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mapreduce.base.Job;
import mapreduce.base.VirtualMachine;
import mapreduce.base.VirtualMachine.SlotType;
import mapreduce.base.VirtualMachine.VmType;
import mapreduce.manager.JobTracker;
import mapreduce.manager.VMManager;
import mapreduce.manager.VMManager.Strategy;
import mapreduce.manager.VMPoolManager;
import mapreduce.manager.VMPriceManager;
import mapreduce.scheduler.DASwithMalleability;
import mapreduce.scheduler.DASwithoutMalleability;
import sun.rmi.runtime.Log;

public class MapReduceUserEngine extends DatacenterBroker {

	public static int BROKER_ID = 0;
	int mSchedulerID;
	JobTracker mJobTracker;
	VMManager mVMManager;
	VMPoolManager mVMPoolManager;
	DASwithMalleability mDASwithMalleability;
	DASwithoutMalleability mDASwithoutMalleability;
	List<Job> mReducePendingJobs;

	public MapReduceUserEngine(String name, int theSchedulerID, JobTracker theJT, VMManager theVM,
			VMPoolManager theVMPOOL) throws Exception {
		super(name);
		mSchedulerID = theSchedulerID;
		mJobTracker = theJT;
		mVMManager = theVM;
		mVMPoolManager = theVMPOOL;
		mDASwithMalleability = new DASwithMalleability(mVMManager, mJobTracker);
		mDASwithoutMalleability = new DASwithoutMalleability(mVMManager, mJobTracker);
		mReducePendingJobs = new ArrayList<Job>();
	}

	@Override
	public void startEntity() {
		super.startEntity();
		schedule(BROKER_ID, 3600, CloudSimTags.CHECK_VM_FOR_REMOVAL);
	}

	// a wrapper for submitVmList
	public void mSubmitVirtualMachineList() {
		List<Vm> slotlist = new ArrayList<Vm>();
		List<VirtualMachine> vmlist = new ArrayList<VirtualMachine>();
		vmlist.addAll(mVMPoolManager.getmReservedVMPoolList());
		vmlist.addAll(mVMPoolManager.getmOnDemandVMPoolList());
		vmlist.addAll(mVMPoolManager.getmSpotVMPoolList());
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
		case CloudSimTags.NEW_JOB:
			processNewJob(ev);
			break;
		case CloudSimTags.LAUNCH_REDUCE:
			processLaunchReduce(ev);
			break;
		case CloudSimTags.CHECK_VM_FOR_REMOVAL:
			processCheckVMForRemoval(ev);
			break;
		case CloudSimTags.DEINIT_FIXED_POOL:
			processDeInitFixedPool();
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

	protected void processDeInitFixedPool() {
		getmVMManager().mDeInitFixedPool();
	}

	protected void createVmInDatacenter(int datacenterId) {
		// send as much vms as possible for this datacenter before trying the
		// next one
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

	protected void processNewJob(SimEvent ev) {
		Job newjob = (Job) ev.getData();
		if(SimulateFailure(newjob)) {
			Random randgen = new Random();
			int randint = (randgen.nextInt(4)+4);
			schedule(MapReduceUserEngine.BROKER_ID, (newjob.getmSubmitTime()+newjob.getmDeadline()/randint),
					CloudSimTags.NEW_JOB, newjob);
			return;
		}
		if(mSchedulerID==1)
			newjob = mDASwithMalleability.ScheduleJob(newjob, 1);
		else
			newjob = mDASwithoutMalleability.ScheduleJob(newjob, 1);
		mSubmitTasksToDatacenter(newjob, 1);
		mReducePendingJobs.add(newjob);
		schedule(mJobTracker.getId(), 0, CloudSimTags.RUNNING_JOB, newjob);
	}

	protected Boolean SimulateFailure(Job theJob) {
		int lower = 0, upper = 2;
		Random randomgen = new Random((long)CloudSim.clock());
		Strategy strategy = mVMManager.getmStrategy();
		if(strategy!=Strategy.OnDemand) {
			if(strategy==Strategy.Hybrid) {
				upper = 1;
				VMPriceManager vmpricemanager = mVMManager.getmVMPriceManager();
				VmType type = vmpricemanager.mPreferredVMType(theJob.getmSubmitTime()+theJob.getmDeadline());
				if(type == VmType.SPOT) {
					Integer randint = randomgen.nextInt(10);
					if(randint>=lower && randint<=upper)
						return true;
					else 
						return false;
				}
				else 
					return false;
			}
			else if(strategy == Strategy.Spot) {
				upper = 4;
				Integer randint = randomgen.nextInt(10);
				if(randint>=lower && randint<=upper)
					return true;
				else 
					return false;
			}
		}
		return false;
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		int slotid = cloudlet.getVmId();
		int taskid = cloudlet.getCloudletId();
		SlotType slottype = mVMManager.getmSlotType(slotid);
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Task " + taskid
				+ " received");
		if(mSchedulerID==1) {
			mDASwithMalleability.mRemoveEntryFromTaskSlotMap(taskid);
			if (mDASwithMalleability.mIsSlotIdle(slotid)) {
				if (slottype == SlotType.MAP) {
					mDASwithMalleability.mRemoveEntryFromMapSlotOccupanceMap(slotid);
					mVMManager.mRemoveMapSlotFromRunningList(slotid);
				} else {
					mDASwithMalleability.mRemoveEntryFromReduceSlotOccupanceMap(slotid);
					mVMManager.mRemoveReduceSlotFromRunningList(slotid);
				}
				Log.printLine(CloudSim.clock() + ": Slot #" + slotid
						+ " sent to idle list");
			}
		}
		else {
			mDASwithoutMalleability.mRemoveEntryFromTaskSlotMap(taskid);
			if (mDASwithoutMalleability.mIsSlotIdle(slotid)) {
				if (slottype == SlotType.MAP) {
					mDASwithoutMalleability.mRemoveEntryFromMapSlotOccupanceMap(slotid);
					mVMManager.mRemoveMapSlotFromRunningList(slotid);
				} else {
					mDASwithoutMalleability.mRemoveEntryFromReduceSlotOccupanceMap(slotid);
					mVMManager.mRemoveReduceSlotFromRunningList(slotid);
				}
				Log.printLine(CloudSim.clock() + ": Slot #" + slotid
						+ " sent to idle list");
			}
		}

		mVMManager.mChangeScheduledTaskCount(slotid, -1);
		schedule(mJobTracker.getId(), 0, CloudSimTags.TASK_COMPLETION, taskid);
		getCloudletReceivedList().add(cloudlet);
		cloudletsSubmitted--;
	}

	public void processLaunchReduce(SimEvent ev) {
		Job newjob = null;
		Long jobid = new Long((long) ev.getData());
		int idx = 0;
		for (Job job : mReducePendingJobs) {
			if (job.__getId() == jobid) {
				if(mSchedulerID==1)
					newjob = mDASwithMalleability.ScheduleJob(job, 2);
				else
					newjob = mDASwithoutMalleability.ScheduleJob(job, 2);
				mSubmitTasksToDatacenter(newjob, 2);
				idx = mReducePendingJobs.indexOf(job);
				break;
			}
		}
		mReducePendingJobs.remove(idx);
	}

	protected void processCheckVMForRemoval(SimEvent ev) {
		getmVMManager().mCheckVMListForDeallocation();
		if (getmJobtracker().getmWaitingJobList().isEmpty()
				&& getmJobtracker().getmRunningJobList().isEmpty()) {
			schedule(BROKER_ID, 0, CloudSimTags.DEINIT_FIXED_POOL);
			return;
		}
		schedule(BROKER_ID, 3600, CloudSimTags.CHECK_VM_FOR_REMOVAL);
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

	/**
	 * @return the mDAScheduler
	 */
	public DASwithMalleability getmDAScheduler() {
		return mDASwithMalleability;
	}

	/**
	 * @param mDAScheduler
	 *            the mDAScheduler to set
	 */
	public void setmDAScheduler(DASwithMalleability mDAScheduler) {
		this.mDASwithMalleability = mDAScheduler;
	}

	/**
	 * @return the mReducePendingJobs
	 */
	public List<Job> getmReducePendingJobs() {
		return mReducePendingJobs;
	}

	/**
	 * @param mReducePendingJobs
	 *            the mReducePendingJobs to set
	 */
	public void setmReducePendingJobs(List<Job> mReducePendingJobs) {
		this.mReducePendingJobs = mReducePendingJobs;
	}

}
