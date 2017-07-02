/**
 * 
 */
package mapreduce.scheduler;

import java.util.ArrayList;
import java.util.List;

import mapreduce.base.Job;
import mapreduce.manager.JobTracker;
import mapreduce.manager.VMManager;
import sun.rmi.runtime.Log;

/**
 * @author Vishal
 *
 */
public class MASwithJohnsonAlgorithm extends Scheduler{

	final static String NAME = "MASwithJohnsonAlgorithm";
	
	public MASwithJohnsonAlgorithm(VMManager theVMManager,
			JobTracker theJobTracker) {
		super(theVMManager, theJobTracker);
	}

	@Override
	public Job ScheduleJob(Job theJob, int type) {
		if (type == 1)
			return ScheduleMapTasksForJob(theJob);
		else
			return ScheduleReduceTasksForJob(theJob);	
	}

	private Job ScheduleReduceTasksForJob(Job theJob) {
		List<Cloudlet> tasklist = theJob.__getRTList();
		List<Integer> slotidlist = mVMManager.getmProductionRSlotsList();
		List<Integer> finalslotlist = new ArrayList<Integer>();
		Integer numwaves = (int) Math.ceil(tasklist.size()/(1.0D*slotidlist.size()));
		for(int i=0;i<slotidlist.size();i++) {
			for(int j=0;j<numwaves;j++) {
				Integer slotid = slotidlist.get(i);
				finalslotlist.add(slotid);
				if(finalslotlist.size()==tasklist.size())
					break;
			}
		}
		theJob.mSetReduceTasks(mAssignSlotsToReduceTasks(tasklist, finalslotlist, finalslotlist.size(), theJob.getmReduceTime()));
		return theJob;
	}

	private List<Cloudlet> mAssignSlotsToReduceTasks(List<Cloudlet> tasklist,
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
						slotid, mReduceSlotOccupanceMap.get(slotid) + c.getCloudletLength());
			mVMManager.mProduction_ChangeScheduledTaskCount(slotid, 1);
			tasklist.set(i, c);
		}
		return tasklist;
	}

	private Job ScheduleMapTasksForJob(Job theJob) {
		List<Cloudlet> tasklist = theJob.__getMTList();
		List<Integer> slotidlist = mVMManager.getmProductionMSlotsList();
		List<Integer> finalslotlist = new ArrayList<Integer>();
		Integer numwaves = (int) Math.ceil(tasklist.size()/(1.0D*slotidlist.size()));
		Log.printLine(CloudSim.clock() +" : "+NAME+ " : " + slotidlist.size() + "|"
				+ tasklist.size() + "|"
				+ numwaves);
		for(int i=0;i<slotidlist.size();i++) {
			for(int j=0;j<numwaves;j++) {
				Integer slotid = slotidlist.get(i);
				finalslotlist.add(slotid);
				if(finalslotlist.size()==tasklist.size())
					break;
			}
		}
		theJob.mSetMapTasks(mAssignSlotsToMapTasks(tasklist, finalslotlist, finalslotlist.size(), theJob.getmMapTime()));
		return theJob;
	}

	private List<Cloudlet> mAssignSlotsToMapTasks(List<Cloudlet> tasklist,
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
			mVMManager.mProduction_ChangeScheduledTaskCount(slotid, 1);
			tasklist.set(i, c);
		}
		return tasklist;
	}

	public List<Job> ScheduleJobBatch(List<Job> joblist) {
		List<Job> finalschedule = new ArrayList<Job>();
		finalschedule.addAll(joblist);
		List<Double> maptimelist = new ArrayList<Double>();
		List<Double> reducetimelist = new ArrayList<Double>();
		String jobids = new String("");
		int i = 0, j = joblist.size()-1;
		joblist = FindJobCompletionTimes(joblist);
		for(Job job:joblist) {
			jobids+=job.__getId()+", ";
			maptimelist.add(job.getmMapTime());
			reducetimelist.add(job.getmReduceTime());
		}
		Log.printLine(CloudSim.clock() +" : "+NAME+ " : joblist received -  " + jobids);
		while(joblist.size()!=0) {
			Double mapmin = getMinFromList(maptimelist);
			Double reducemin = getMinFromList(reducetimelist);
			if(mapmin<reducemin) {
				int idx = maptimelist.indexOf(mapmin);
				Job job = joblist.get(idx);
				finalschedule.set(i++, job);
				maptimelist.remove(idx);
				reducetimelist.remove(idx);
				joblist.remove(job);
			}
			else {
				int idx = reducetimelist.indexOf(reducemin);
				Job job = joblist.get(idx);
				finalschedule.set(j--, job);
				maptimelist.remove(idx);
				reducetimelist.remove(idx);
				joblist.remove(job);
			}
		}
		jobids = "";
		for(Job job:finalschedule) {
			jobids+=job.__getId()+", ";
		}
		Log.printLine(CloudSim.clock() +" : "+NAME+ " : Final schedule -  " + jobids);
		return finalschedule;
	}

	private List<Job> FindJobCompletionTimes(List<Job> joblist) {
		int nummapslots = mVMManager.getmProductionMSlotsList().size();
		int numreduceslots = mVMManager.getmProductionRSlotsList().size();
		for(Job job:joblist) {
			int i = joblist.indexOf(job);
			job.setmNumMapSlots(new Long(nummapslots));
			job.setmNumReduceSlots(new Long(numreduceslots));
			Double maptime = Math.ceil(job.getmNumMapTasks()/job.getmNumMapSlots())*job.getmAvgMapTime();
			Double reducetime = Math.ceil(job.getmNumReduceTasks()/job.getmNumReduceSlots())*job.getmAvgReduceTime();
			job.setmMapTime(maptime);
			job.setmReduceTime(reducetime);
			joblist.set(i, job);
		}
		return joblist;
	}

	private Double getMinFromList(List<Double> list) {
		Double min = list.get(0);
		for(Double val:list) {
			if(min>val)
				min = val;
		}
		return min;
	}

}
