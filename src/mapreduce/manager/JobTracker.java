package mapreduce.manager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import mapreduce.base.Job;
import mapreduce.base.Job.Priority;
import mapreduce.engine.MapReduceProductionEngine;
import mapreduce.engine.MapReduceUserEngine;
import mapreduce.helper.HelperFunctions;
import sun.rmi.runtime.Log;

public class JobTracker extends SimEntity {

	public enum TaskType {
		MAP, REDUCE
	};

	List<Job> mWaitingJobList;
	List<Job> mRunningJobList;
	List<Job> mFinishedJobList;
	List<Job> mProductionBatch;
	List<Job> mProductionFinishedJobs;
	Boolean mBatchStarted = false;
	Double mBatchStartTime = 0D;
	Double mMakespan = 0D;
	HashMap<Integer, Long> mTaskJobMap = new HashMap<Integer, Long>();
	HashMap<Integer, TaskType> mTaskTypeMap = new HashMap<Integer, TaskType>();
	HashMap<Long, Boolean> mReducePending = new HashMap<Long, Boolean>();
	String input_data = "resources\\";
	String production_input_path = "resources\\";

	public JobTracker(String name) {
		super(name);
		mWaitingJobList = new ArrayList<Job>();
		mRunningJobList = new ArrayList<Job>();
		mFinishedJobList = new ArrayList<Job>();
		mProductionBatch = new ArrayList<Job>();
		mProductionFinishedJobs = new ArrayList<Job>();
	}

	public void mAddToJobList(Job theJob) {
		this.mWaitingJobList.add(theJob);
		this.mReducePending.put(theJob.__getId(), true);
		for (Cloudlet task : theJob.__getMTList()) {
			mTaskJobMap.put(new Integer(task.getCloudletId()),
					new Long(theJob.__getId()));
			mTaskTypeMap.put(new Integer(task.getCloudletId()), TaskType.MAP);
		}

		for (Cloudlet task : theJob.__getRTList()) {
			mTaskJobMap.put(new Integer(task.getCloudletId()),
					new Long(theJob.__getId()));
			mTaskTypeMap
			.put(new Integer(task.getCloudletId()), TaskType.REDUCE);
		}
	}

	public void mProduction_AddToJobList(Job theJob) {
		this.mProductionBatch.add(theJob);
		this.mReducePending.put(theJob.__getId(), true);
		for (Cloudlet task : theJob.__getMTList()) {
			mTaskJobMap.put(new Integer(task.getCloudletId()),
					new Long(theJob.__getId()));
			mTaskTypeMap.put(new Integer(task.getCloudletId()), TaskType.MAP);
		}

		for (Cloudlet task : theJob.__getRTList()) {
			mTaskJobMap.put(new Integer(task.getCloudletId()),
					new Long(theJob.__getId()));
			mTaskTypeMap
			.put(new Integer(task.getCloudletId()), TaskType.REDUCE);
		}
	}

	@Override
	public void startEntity() {
		Log.printLine(getName() + " is starting...");
		mSendJobs();
	}

	private void mSendJobs() {
		try {
			mSendUserJobs();
			mSendProductionJobs();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mSendUserJobs() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(input_data));
		String line;
		String vals[];
		line = br.readLine();
		while ((line = br.readLine()) != null) {
			line = line.replace("\n", "");
			vals = line.split("\t");
			String jobname = vals[0];
			Double arrivaltime = Double.parseDouble(vals[1]);
			Double avgmt = Double.parseDouble(vals[7]);
			Double avgrt = Double.parseDouble(vals[3]);
			Long nummaptasks = (long) Math.ceil(Double.parseDouble(vals[4]));
			Long numreducetasks = Long.parseLong(vals[5]);
			Double minmaptime = Double.parseDouble(vals[6]);
			Double maxmaptime = Double.parseDouble(vals[7]);
			Double deadline = Double.parseDouble(vals[8]);
			Job job = HelperFunctions.sCreateJob(jobname,
					MapReduceUserEngine.BROKER_ID, Priority.LOW, nummaptasks,
					numreducetasks, avgmt, avgrt, minmaptime, maxmaptime,
					deadline, arrivaltime);
			mAddToJobList(job);
			Log.printLine(job.__getId() + " :" + job);
			schedule(MapReduceUserEngine.BROKER_ID, arrivaltime,
					CloudSimTags.NEW_JOB, job);
		}
		br.close();
	}

	private void mSendProductionJobs() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(
				"resources\\production_job_data_1.txt"));
		String line;
		String vals[];
		line = br.readLine();
		List<Job> joblist = new ArrayList<Job>();
		while ((line = br.readLine()) != null) {
			line = line.replace("\n", "");
			vals = line.split("\t");
			Long batchid = Long.parseLong(vals[0]);
			String jobname = vals[1];
			Double arrivaltime = 1000.0D;
			Double avgmt = Double.parseDouble(vals[2]);
			Double avgrt = Double.parseDouble(vals[3]);
			Long nummaptasks = (long) Math.ceil(Double.parseDouble(vals[4]));
			Long numreducetasks = Long.parseLong(vals[5]);
			Double minmaptime = Double.parseDouble(vals[6]);
			Double maxmaptime = Double.parseDouble(vals[7]);
			Double deadline = 0D;
			Job job = HelperFunctions.sCreateJob(jobname,
					MapReduceProductionEngine.BROKER_ID, Priority.LOW, nummaptasks,
					numreducetasks, avgmt, avgrt, minmaptime, maxmaptime,
					deadline, arrivaltime);
			job.setmBatchId(batchid);
			mProduction_AddToJobList(job);
			joblist.add(job);
			Log.printLine(job.__getId() + " :" + job);
		}
		schedule(MapReduceProductionEngine.BROKER_ID, 1000.0D,
				CloudSimTags.NEW_BATCH, joblist);
		br.close();
	}

	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		case CloudSimTags.RUNNING_JOB:
			processRunningJob(ev);
			break;
		case CloudSimTags.TASK_COMPLETION:
			if (ev.getSource() == MapReduceUserEngine.BROKER_ID)
				processCompletedTasks(ev);
			else if(ev.getSource()==MapReduceProductionEngine.BROKER_ID)
				processProductionCompletedTasks(ev);
			break;
		default:
			Log.print("Unknown event type.\n");
			break;
		}
	}

	private void processRunningJob(SimEvent ev) {
		Job job = (Job) ev.getData();
		if (ev.getSource() == MapReduceUserEngine.BROKER_ID)
			mSendToRunningJobList(job.__getId(), ev.eventTime());
		else if(ev.getSource() == MapReduceProductionEngine.BROKER_ID)
			mSendToProductionRunningJobList(job.__getId(), ev.eventTime());
	}

	private void processCompletedTasks(SimEvent ev) {
		Integer taskid = (int) ev.getData();
		Double etime = ev.eventTime();
		long jobid = mTaskJobMap.get(taskid);
		for (Job job : mRunningJobList) {
			if (job.__getId() == jobid) {
				job.setmFinishedTasks(job.getmFinishedTasks() + 1);
				if (mReducePending.get(jobid)) {
					if (job.getmFinishedTasks() == job.getmNumMapTasks()) {
						schedule(MapReduceUserEngine.BROKER_ID, 0,
								CloudSimTags.LAUNCH_REDUCE, jobid);
						mReducePending.put(jobid, false);
					}
				}
				if (job.__isJobFinished())
					mSendToFinishedJobList(jobid, etime);
				break;
			}
		}
	}


	private void processProductionCompletedTasks(SimEvent ev) {
		Integer taskid = (int) ev.getData();
		Double etime = ev.eventTime();
		long jobid = mTaskJobMap.get(taskid);
		for (Job job : mProductionBatch) {
			if (job.__getId() == jobid) {
				job.setmFinishedTasks(job.getmFinishedTasks() + 1);
				if (mReducePending.get(jobid)) {
					if (job.getmFinishedTasks() == job.getmNumMapTasks()) {
						schedule(MapReduceProductionEngine.BROKER_ID, 0,
								CloudSimTags.LAUNCH_REDUCE, jobid);
						mReducePending.put(jobid, false);
					}
				}
				if (job.__isJobFinished())
					mSendToProductionFinishedList(jobid, etime);
				break;
			}
		}
	}

	public void mSendToProductionFinishedList(long jobid, Double endtime) {
		for (Job job : mProductionBatch) {
			if (jobid == job.__getId()) {
				job.setmFinishTime(endtime);
				mProductionBatch.remove(job);
				if(mProductionBatch.size()==0)
					mMakespan = endtime-mBatchStartTime;
				mProductionFinishedJobs.add(job);
				Log.print(CloudSim.clock() + ": Job " + job.__getId()
						+ " added to production finished list at time:"
						+ job.getmFinishTime() + " \n");
				Log.print(CloudSim.clock() + ": Production finished queue: "
						+ mProductionFinishedJobs.toString() + "\n");
				Log.print(CloudSim.clock() + ": Production Running queue: "
						+ mProductionBatch.toString() + "\n");
				break;
			}
		}
	}

	public void mSendToFinishedJobList(long jobid, Double endtime) {
		for (Job job : mRunningJobList) {
			if (jobid == job.__getId()) {
				job.setmFinishTime(endtime);
				mRunningJobList.remove(job);
				mFinishedJobList.add(job);
				Log.print(CloudSim.clock() + ": Job " + job.__getId()
						+ " added to finished list at time:"
						+ job.getmFinishTime() + " \n");
				Log.print(CloudSim.clock() + ": Finished queue: "
						+ mFinishedJobList.toString() + "\n");
				Log.print(CloudSim.clock() + ": Running queue: "
						+ mRunningJobList.toString() + "\n");
				break;
			}
		}
		if (mWaitingJobList.isEmpty() && mRunningJobList.isEmpty())
			schedule(MapReduceUserEngine.BROKER_ID, 0, CloudSimTags.SHUTDOWN);
	}

	public void mSendToProductionRunningJobList(long jobid, Double starttime) {
		for (Job job : mProductionBatch) {
			if (jobid == job.__getId()) {
				if(mBatchStarted==false) {
					mBatchStartTime = starttime;
					mBatchStarted = true;
				}
				job.setmStartTime(starttime);
				Log.printLine(CloudSim.clock() + ": Production Job " + job.__getId()
						+ " marked as running at - "+starttime);
				break;
			}
		}
	}

	public void mSendToRunningJobList(long jobid, Double starttime) {
		for (Job job : mWaitingJobList) {
			if (jobid == job.__getId()) {
				job.setmStartTime(starttime);
				mWaitingJobList.remove(job);
				mRunningJobList.add(job);
				Log.print(CloudSim.clock() + ": Job " + job.__getId()
						+ " added to running queue.\n");
				Log.print(CloudSim.clock() + ": Running queue: "
						+ mRunningJobList.toString() + "\n");
				Log.print(CloudSim.clock() + ": Waiting queue: "
						+ mWaitingJobList.toString() + "\n");
				break;
			}
		}
	}

	@Override
	public void shutdownEntity() {
		// TODO Auto-generated method stub
		Log.printLine(getName() + " is shutting down...");

	}

	/**
	 * @return the mFinishedJobList
	 */
	public List<Job> getmFinishedJobList() {
		return mFinishedJobList;
	}

	/**
	 * @param mFinishedJobList
	 *            the mFinishedJobList to set
	 */
	public void setmFinishedJobList(List<Job> mFinishedJobList) {
		this.mFinishedJobList = mFinishedJobList;
	}

	/**
	 * @return the mTaskJobMap
	 */
	public HashMap<Integer, Long> getmTaskJobMap() {
		return mTaskJobMap;
	}

	/**
	 * @param mTaskJobMap
	 *            the mTaskJobMap to set
	 */
	public void setmTaskJobMap(HashMap<Integer, Long> mTaskJobMap) {
		this.mTaskJobMap = mTaskJobMap;
	}

	/**
	 * @return the mTaskTypeMap
	 */
	public HashMap<Integer, TaskType> getmTaskTypeMap() {
		return mTaskTypeMap;
	}

	/**
	 * @param mTaskTypeMap
	 *            the mTaskTypeMap to set
	 */
	public void setmTaskTypeMap(HashMap<Integer, TaskType> mTaskTypeMap) {
		this.mTaskTypeMap = mTaskTypeMap;
	}

	/**
	 * @return the mWaitingJobList
	 */
	public List<Job> getmWaitingJobList() {
		return mWaitingJobList;
	}

	/**
	 * @param mWaitingJobList
	 *            the mWaitingJobList to set
	 */
	public void setmWaitingJobList(List<Job> mWaitingJobList) {
		this.mWaitingJobList = mWaitingJobList;
	}

	/**
	 * @return the mRunningJobList
	 */
	public List<Job> getmRunningJobList() {
		return mRunningJobList;
	}

	/**
	 * @param mRunningJobList
	 *            the mRunningJobList to set
	 */
	public void setmRunningJobList(List<Job> mRunningJobList) {
		this.mRunningJobList = mRunningJobList;
	}

	/**
	 * @return the mReducePending
	 */
	public HashMap<Long, Boolean> getmReducePending() {
		return mReducePending;
	}

	/**
	 * @param mReducePending
	 *            the mReducePending to set
	 */
	public void setmReducePending(HashMap<Long, Boolean> mReducePending) {
		this.mReducePending = mReducePending;
	}

	/**
	 * @return the input_data
	 */
	public String getInput_data() {
		return input_data;
	}

	/**
	 * @param input_data
	 *            the input_data to set
	 */
	public void setInput_data(String input_data) {
		this.input_data = this.input_data + input_data + ".txt";
	}

	/**
	 * @return the mProductionBatch
	 */
	public List<Job> getmProductionBatch() {
		return mProductionBatch;
	}

	/**
	 * @param mProductionBatch the mProductionBatch to set
	 */
	public void setmProductionBatch(List<Job> mProductionBatch) {
		this.mProductionBatch = mProductionBatch;
	}

	/**
	 * @return the mProductionFinishedJobs
	 */
	public List<Job> getmProductionFinishedJobs() {
		return mProductionFinishedJobs;
	}

	/**
	 * @param mProductionFinishedJobs the mProductionFinishedJobs to set
	 */
	public void setmProductionFinishedJobs(List<Job> mProductionFinishedJobs) {
		this.mProductionFinishedJobs = mProductionFinishedJobs;
	}

	/**
	 * @return the production_input_path
	 */
	public String getProduction_input_path() {
		return production_input_path;
	}

	/**
	 * @param production_input_path the production_input_path to set
	 */
	public void setProduction_input_path(String production_input_path) {
		this.production_input_path =this.production_input_path + production_input_path + ".txt";
	}

	/**
	 * @return the mMakespan
	 */
	public Double getmMakespan() {
		return mMakespan;
	}

	/**
	 * @param mMakespan the mMakespan to set
	 */
	public void setmMakespan(Double mMakespan) {
		this.mMakespan = mMakespan;
	}

}
