package mapreduce.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mapreduce.base.Job;
import mapreduce.base.VirtualMachine.VmType;
import mapreduce.manager.JobTracker;
import mapreduce.manager.VMManager;
import sun.rmi.runtime.Log;

public class DASwithMalleability extends Scheduler {

	final static String NAME = "DASwithMalleability";
	final static Double NUMOFMAPSLOTS = 2.0D;
	final static Double NUMOFREDUCESLOTS = 2.0D;

	public DASwithMalleability(VMManager theVMManager,
			JobTracker theJobTracker) {
		super(theVMManager, theJobTracker);
	}

	@Override
	public Job ScheduleJob(Job theJob, int type) {
		if (type == 1) {
			long[] requiredslots = mGetNumberOfMapSlots(theJob);
			theJob.setmNumMapSlots(requiredslots[0]);
			theJob.setmNumReduceSlots(requiredslots[1]);
			return ScheduleMapTasksForJob(theJob);
		} else
			return ScheduleReduceTasksForJob(theJob);
	}

	public Job ScheduleMapTasksForJob(Job theJob) {
		if (theJob.getmNumMapTasks() == 0)
			return theJob;
		long reqnummapslots = theJob.getmNumMapSlots();
		List<Integer> mapslotidlist = new ArrayList<Integer>();
		long numtasks = theJob.getmNumMapTasks();
		long totalnumslots = numtasks;
		long numwaves = (long) Math.ceil(numtasks / (1.0D * reqnummapslots));
		Double jobendtime = theJob.getmExpectedFinishTime();
		Double maplength = theJob.getmAvgMapTime() * numwaves;
		Double mapendtime = CloudSim.clock() + maplength;
		Double spotbid = mVMManager.getmSpotBid(jobendtime);
		Log.printLine(CloudSim.clock() +" : "+NAME+" :Number of waves:" + numwaves);
		Log.printLine(CloudSim.clock() +" : "+NAME+" :Number of slots:" + totalnumslots);

		/* Map Part */
		totalnumslots = mGetMapSlotsFromIdleSlotList(mapslotidlist, numwaves,
				totalnumslots, spotbid);
		Log.printLine(CloudSim.clock() +" : "+NAME+ " :Number of slots left after idle slot allocation: " + totalnumslots);
		if (totalnumslots > 0) {
			totalnumslots = mGetMapSlotsFromRunningJobs(totalnumslots,
					mapendtime, mapslotidlist, spotbid, theJob.getmAvgMapTime());
			Log.printLine(CloudSim.clock() +" : "+NAME+ " :Number of slots left after running slot allocation: " + totalnumslots);
			if (totalnumslots > 0) {
				mRequestNewMapSlots(totalnumslots, numwaves, jobendtime);
				totalnumslots = mGetMapSlotsFromIdleSlotList(mapslotidlist,
						numwaves, totalnumslots, spotbid);
				Log.printLine(CloudSim.clock() +" : "+NAME+ " :Number of slots left after new slot allocation: " + totalnumslots);
			}
		}
		theJob.mSetMapTasks(mAssignSlotsToMapTasks(theJob.__getMTList(),
				mapslotidlist, mapslotidlist.size(), jobendtime));
		return theJob;
	}

	public Job ScheduleReduceTasksForJob(Job theJob) {
		if (theJob.getmNumReduceTasks() == 0)
			return theJob;
		long reqnumreduceslots = theJob.getmNumReduceSlots();
		List<Integer> reduceslotidlist = new ArrayList<Integer>();
		long numtasks = theJob.getmNumReduceTasks();
		long totalnumslots = numtasks;
		long numwaves = (long) Math.ceil(numtasks / (1.0D * reqnumreduceslots));
		Double jobendtime = theJob.getmExpectedFinishTime();
		Double reducelength = theJob.getmAvgReduceTime() * numwaves;
		Double reduceendtime = CloudSim.clock() + reducelength;
		Double spotbid = mVMManager.getmSpotBid(jobendtime);

		/* Reduce Part */
		totalnumslots = mGetReduceSlotsFromIdleSlotList(reduceslotidlist,
				numwaves, totalnumslots, spotbid);
		if (totalnumslots > 0) {
			totalnumslots = mGetReduceSlotsFromRunningJobs(totalnumslots,
					reduceendtime, reduceslotidlist, spotbid,
					theJob.getmAvgReduceTime());
			if (totalnumslots > 0) {
				mRequestNewReduceSlots(totalnumslots, numwaves, jobendtime);
				totalnumslots = mGetReduceSlotsFromIdleSlotList(
						reduceslotidlist, numwaves, totalnumslots, spotbid);
			}
		}
		theJob.mSetReduceTasks(mAssignSlotsToReduceTasks(theJob.__getRTList(),
				reduceslotidlist, reduceslotidlist.size(), jobendtime));
		return theJob;
	}

	private Double mRequestNewMapSlots(long totalnumslots, long numwaves,
			Double endtime) {
		Double nummapslots = Math.ceil(totalnumslots / (1.0D * numwaves));
		Integer numvms = (int) Math.ceil(nummapslots / NUMOFMAPSLOTS);
		for (int i = 0; i < numvms; i++)
			mVMManager.mAllocateVMFromPool(endtime);
		return nummapslots;
	}

	private Double mRequestNewReduceSlots(long totalnumslots, long numwaves,
			Double endtime) {
		Double numreduceslots = Math.ceil(totalnumslots / (1.0D * numwaves));
		Integer numvms = (int) Math.ceil(numreduceslots / NUMOFREDUCESLOTS);
		for (int i = 0; i < numvms; i++)
			mVMManager.mAllocateVMFromPool(endtime);
		return numreduceslots;
	}

	public Long mGetMapSlotsFromRunningJobs(Long totalnumslots, Double endtime,
			List<Integer> slotidlist, Double bid, Double avgmt) {
		Double effectivenumofmaps = 0D;
		for (Map.Entry<Integer, Double> entry : mMapSlotOccupanceMap.entrySet()) {
			if (endtime > entry.getValue()) {
				int slotid = entry.getKey();
				if (mVMManager.getmSlotVmType(slotid) == VmType.SPOT) {
					if (mVMManager.getmSlotBid(slotid) >= bid) {
						effectivenumofmaps = Math.floor((endtime - entry
								.getValue()) / avgmt);
						if (effectivenumofmaps >= 1) {
							while (effectivenumofmaps > 0) {
								slotidlist.add(entry.getKey());
								effectivenumofmaps--;
								totalnumslots--;
								if (totalnumslots == 0)
									break;
							}
						}
					}
				} else {
					effectivenumofmaps = Math
							.floor((endtime - entry.getValue()) / avgmt);
					if (effectivenumofmaps >= 1) {
						while (effectivenumofmaps > 0) {
							slotidlist.add(entry.getKey());
							effectivenumofmaps--;
							totalnumslots--;
							if (totalnumslots == 0)
								break;
						}
					}
				}
			}
			if (totalnumslots == 0)
				break;
		}
		return totalnumslots;
	}

	public Long mGetReduceSlotsFromRunningJobs(Long totalnumslots,
			Double endtime, List<Integer> slotidlist, Double bid, Double avgrt) {
		Double effectivenumofreduce = 0D;
		for (Map.Entry<Integer, Double> entry : mReduceSlotOccupanceMap
				.entrySet()) {
			if (endtime > entry.getValue()) {
				int slotid = entry.getKey();
				if (mVMManager.getmSlotVmType(slotid) == VmType.SPOT) {
					if (mVMManager.getmSlotBid(slotid) >= bid) {
						effectivenumofreduce = Math.floor((endtime - entry
								.getValue()) / avgrt);
						if (effectivenumofreduce >= 1) {
							while (effectivenumofreduce > 0) {
								slotidlist.add(entry.getKey());
								effectivenumofreduce--;
								totalnumslots--;
								if (totalnumslots == 0)
									break;
							}
						}
					}
				} else {
					effectivenumofreduce = Math.floor((endtime - entry
							.getValue()) / avgrt);
					if (effectivenumofreduce >= 1) {
						while (effectivenumofreduce > 0) {
							slotidlist.add(entry.getKey());
							effectivenumofreduce--;
							totalnumslots--;
							if (totalnumslots == 0)
								break;
						}
					}
				}
			}
			if (totalnumslots == 0)
				break;
		}
		return totalnumslots;
	}

	public long mGetMapSlotsFromIdleSlotList(List<Integer> slotidlist,
			long numwaves, long totalnumslots, Double bid) {
		List<Integer> idlemapslots = new ArrayList<Integer>();
		idlemapslots.addAll(mVMManager.getmIdleMSlotsList());
		int i = 0;
		while (totalnumslots > 0 && i < idlemapslots.size()) {
			int slotid = idlemapslots.get(i);
			if (mVMManager.getmSlotVmType(slotid) == VmType.SPOT) {
				if (mVMManager.getmSlotBid(slotid) >= bid) {
					for (int j = 0; j < numwaves; j++) {
						mVMManager.mAddMapSlotToRunningList(slotid);
						slotidlist.add(new Integer(slotid));
						totalnumslots--;
					}
				}
			} else {
				for (int j = 0; j < numwaves; j++) {
					mVMManager.mAddMapSlotToRunningList(slotid);
					slotidlist.add(new Integer(slotid));
					totalnumslots--;
				}
			}
			i++;
		}
		return totalnumslots;
	}

	public long mGetReduceSlotsFromIdleSlotList(List<Integer> slotidlist,
			long numwaves, long totalnumslots, Double bid) {
		List<Integer> idlereduceslots = new ArrayList<Integer>();
		idlereduceslots.addAll(mVMManager.getmIdleRSlotsList());
		int i = 0;
		while (totalnumslots > 0 && i < idlereduceslots.size()) {
			int slotid = idlereduceslots.get(i);
			if (mVMManager.getmSlotVmType(slotid) == VmType.SPOT) {
				if (mVMManager.getmSlotBid(slotid) >= bid) {
					for (int j = 0; j < numwaves; j++) {
						mVMManager.mAddReduceSlotToRunningList(slotid);
						slotidlist.add(new Integer(slotid));
						totalnumslots--;
					}
				}
			} else {
				for (int j = 0; j < numwaves; j++) {
					mVMManager.mAddReduceSlotToRunningList(slotid);
					slotidlist.add(new Integer(slotid));
					totalnumslots--;
				}
			}
			i++;
		}
		return totalnumslots;
	}

	public List<Cloudlet> mAssignSlotsToMapTasks(List<Cloudlet> tasklist,
			List<Integer> slotidlist, long numslots, Double jobendtime) {
		Log.printLine(CloudSim.clock() +" : "+NAME+ " : " + slotidlist.size() + "|"
				+ tasklist.size());
		for (int i = 0; i < tasklist.size(); i++) {
			Cloudlet c = tasklist.get(i);
			Integer slotid = slotidlist.get(i);
			c.setVmId(slotid);
			this.mRunningTaskSlotMap.put(c.getCloudletId(), slotid);
			if (!mMapSlotOccupanceMap.containsKey(slotid))
				this.mMapSlotOccupanceMap.put(slotid,
						CloudSim.clock() + c.getCloudletLength());
			else
				this.mMapSlotOccupanceMap.put(
						slotid, mMapSlotOccupanceMap.get(slotid) + c.getCloudletLength());
			mVMManager.mChangeScheduledTaskCount(slotid, 1);
			tasklist.set(i, c);
		}
		return tasklist;
	}

	public List<Cloudlet> mAssignSlotsToReduceTasks(List<Cloudlet> tasklist,
			List<Integer> slotidlist, long numslots, Double jobendtime) {
		Log.printLine(CloudSim.clock() +" : "+NAME+ " : " + slotidlist.size() + "|"
				+ tasklist.size());
		for (int i = 0; i < tasklist.size(); i++) {
			Cloudlet c = tasklist.get(i);
			Integer slotid = slotidlist.get(i);
			c.setVmId(slotid);
			this.mRunningTaskSlotMap.put(c.getCloudletId(), slotid);
			if (!mReduceSlotOccupanceMap.containsKey(slotid))
				this.mReduceSlotOccupanceMap.put(slotid,
						CloudSim.clock() + c.getCloudletLength());
			else
				this.mReduceSlotOccupanceMap.put(
						slotid, mReduceSlotOccupanceMap.get(slotid)
								+ c.getCloudletLength());
			mVMManager.mChangeScheduledTaskCount(slotid, 1);
			tasklist.set(i, c);
		}
		return tasklist;
	}

	public long[] mGetNumberOfMRSlots(Job theJob) {
		long[] slots = new long[2];
		Double const_a = Math.sqrt(theJob.getmNumMapTasks()
				* theJob.getmAvgMapTime());
		Double const_b = Math.sqrt(theJob.getmNumReduceTasks()
				* theJob.getmAvgReduceTime());
		Double const_c = (const_a + const_b) / (theJob.getmDeadline());
		slots[0] = (long) Math.ceil(const_a * const_c);
		slots[1] = (long) Math.ceil(const_b * const_c);
		if (slots[1] > theJob.getmNumReduceTasks())
			slots = mGetNumberOfMapSlots(theJob);
		if (slots[0] > theJob.getmNumMapTasks())
			slots[0] = theJob.getmNumMapTasks();
		return slots;
	}

	public long[] mGetNumberOfMapSlots(Job theJob) {
		long[] slots = new long[2];
		Double const_a = theJob.getmNumMapTasks() * theJob.getmAvgMapTime();
		Double const_b = theJob.getmAvgReduceTime();
		Double const_c = (double) Math.round(theJob.getmDeadline() - (const_b));
		slots[0] = (long) Math.ceil(const_a / const_c);
		slots[1] = (long) theJob.getmNumReduceTasks();
		return slots;
	}
}
