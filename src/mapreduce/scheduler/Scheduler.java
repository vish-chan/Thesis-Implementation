package mapreduce.scheduler;

import java.util.HashMap;
import java.util.Map;

import mapreduce.base.Job;
import mapreduce.manager.JobTracker;
import mapreduce.manager.VMManager;

public abstract class Scheduler {
	
	Map<Integer,Integer> mRunningTaskSlotMap;
	Map<Integer,Double> mMapSlotOccupanceMap;
	Map<Integer,Double> mReduceSlotOccupanceMap;
	VMManager mVMManager;
	JobTracker mJobtracker;
	
	public Scheduler(VMManager theVMManager, JobTracker theJobTracker) {
		mRunningTaskSlotMap = new HashMap<Integer, Integer>();
		mMapSlotOccupanceMap = new HashMap<Integer, Double>();
		mReduceSlotOccupanceMap = new HashMap<Integer, Double>();
		this.mVMManager = theVMManager;
		this.mJobtracker = theJobTracker;
	}
	
	public abstract Job ScheduleJob(Job theJob, int type);
	
	public void mRemoveEntryFromTaskSlotMap(Integer taskid) {
		mRunningTaskSlotMap.remove(taskid);
	}
	
	public void mRemoveEntryFromMapSlotOccupanceMap(Integer slotid) {
		mMapSlotOccupanceMap.remove(slotid);
	}
	
	public void mRemoveEntryFromReduceSlotOccupanceMap(Integer slotid) {
		mReduceSlotOccupanceMap.remove(slotid);
	}
	
	public Boolean mIsSlotIdle(Integer slotid) {
		if(mRunningTaskSlotMap.containsValue(slotid))
			return false;
		return true;
	}


	/**
	 * @return the mRunningTaskSlotMap
	 */
	public Map<Integer, Integer> getmRunningTaskSlotMap() {
		return mRunningTaskSlotMap;
	}

	/**
	 * @param mRunningTaskSlotMap the mRunningTaskSlotMap to set
	 */
	public void setmRunningTaskSlotMap(Map<Integer, Integer> mRunningTaskSlotMap) {
		this.mRunningTaskSlotMap = mRunningTaskSlotMap;
	}

	/**
	 * @return the mMapSlotOccupanceMap
	 */
	public Map<Integer, Double> getmMapSlotOccupanceMap() {
		return mMapSlotOccupanceMap;
	}

	/**
	 * @param mMapSlotOccupanceMap the mMapSlotOccupanceMap to set
	 */
	public void setmMapSlotOccupanceMap(Map<Integer, Double> mMapSlotOccupanceMap) {
		this.mMapSlotOccupanceMap = mMapSlotOccupanceMap;
	}

	/**
	 * @return the mReduceSlotOccupanceMap
	 */
	public Map<Integer, Double> getmReduceSlotOccupanceMap() {
		return mReduceSlotOccupanceMap;
	}

	/**
	 * @param mReduceSlotOccupanceMap the mReduceSlotOccupanceMap to set
	 */
	public void setmReduceSlotOccupanceMap(
			Map<Integer, Double> mReduceSlotOccupanceMap) {
		this.mReduceSlotOccupanceMap = mReduceSlotOccupanceMap;
	}

	/**
	 * @return the mVMManager
	 */
	public VMManager getmVMManager() {
		return mVMManager;
	}

	/**
	 * @param mVMManager the mVMManager to set
	 */
	public void setmVMManager(VMManager mVMManager) {
		this.mVMManager = mVMManager;
	}

	/**
	 * @return the mJobtracker
	 */
	public JobTracker getmJobtracker() {
		return mJobtracker;
	}

	/**
	 * @param mJobtracker the mJobtracker to set
	 */
	public void setmJobtracker(JobTracker mJobtracker) {
		this.mJobtracker = mJobtracker;
	}
	
	
}
