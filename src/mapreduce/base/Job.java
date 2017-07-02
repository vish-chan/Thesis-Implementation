package mapreduce.base;

import java.util.ArrayList;
import java.util.List;

public class Job {
	String mName;
	long mUserId;
	long mId;
	long mBatchId;

	public enum Priority {
		HIGH, LOW;
	};

	Priority mPriority;
	List<Cloudlet> mMTList;
	List<Cloudlet> mRTList;
	Long mNumMapSlots;
	Long mNumReduceSlots;
	Double mAvgMapTime;
	Double mAvgReduceTime;
	Double mFirstReduceTime;
	Double mDeadline;
	Double mSubmitTime;
	Double mStartTime;
	Double mExpectedFinishTime;
	Double mFinishTime;
	Double mMapTime;
	Double mReduceTime;
	long mFinishedTasks;

	public Job(String theName, long theUserId, long theId,
			Priority thePriority, Double theAvgMT, Double theAvgRT,
			Double theDeadline, Double theSubmitTime) {
		this.mUserId = theUserId;
		this.mName = theName;
		this.mId = theId;
		this.mPriority = thePriority;
		this.mMTList = new ArrayList<Cloudlet>();
		this.mRTList = new ArrayList<Cloudlet>();
		this.mAvgMapTime = theAvgMT;
		this.mAvgReduceTime = theAvgRT;
		this.mFirstReduceTime = theAvgRT;
		this.mDeadline = theDeadline;
		this.mFinishedTasks = 0;
		this.mNumMapSlots = 0L;
		this.mNumReduceSlots = 0L;
		this.mMapTime = 0D;
		this.mReduceTime = 0D;
		this.mSubmitTime = theSubmitTime;
		this.mExpectedFinishTime = mSubmitTime+mDeadline;
		this.mBatchId = -1;
	}

	public void mSetMapTasks(List<Cloudlet> theMT) {
		this.mMTList = theMT;
	}

	public void mSetReduceTasks(List<Cloudlet> theRT) {
		this.mRTList = theRT;
	}

	public int getmNumMapTasks() {
		return mMTList.size();
	}

	public int getmNumReduceTasks() {
		return mRTList.size();
	}

	public boolean __isJobFinished() {
		return ((mMTList.size() + mRTList.size()) == mFinishedTasks);
	}

	public void mIncreaseFinishedTasksCounter(long val) {
		mFinishedTasks += val;
	}

	public List<Cloudlet> __getMTList() {
		return mMTList;
	}

	public List<Cloudlet> __getRTList() {
		return mRTList;
	}

	public long __getId() {
		return this.mId;
	}

	/**
	 * @return the mUserId
	 */
	public long getmUserId() {
		return mUserId;
	}

	/**
	 * @param mUserId
	 *            the mUserId to set
	 */
	public void setmUserId(long mUserId) {
		this.mUserId = mUserId;
	}

	/**
	 * @return the mPriority
	 */
	public Priority getmPriority() {
		return mPriority;
	}

	/**
	 * @param mPriority
	 *            the mPriority to set
	 */
	public void setmPriority(Priority mPriority) {
		this.mPriority = mPriority;
	}

	/**
	 * @return the mAvgMapTime
	 */
	public Double getmAvgMapTime() {
		return mAvgMapTime;
	}

	/**
	 * @param mAvgMapTime
	 *            the mAvgMapTime to set
	 */
	public void setmAvgMapTime(Double mAvgMapTime) {
		this.mAvgMapTime = mAvgMapTime;
	}

	/**
	 * @return the mAvgReduceTime
	 */
	public Double getmAvgReduceTime() {
		return mAvgReduceTime;
	}

	/**
	 * @param mAvgReduceTime
	 *            the mAvgReduceTime to set
	 */
	public void setmAvgReduceTime(Double mAvgReduceTime) {
		this.mAvgReduceTime = mAvgReduceTime;
	}

	/**
	 * @return the mFinishedTasks
	 */
	public long getmFinishedTasks() {
		return mFinishedTasks;
	}

	/**
	 * @param mFinishedTasks
	 *            the mFinishedTasks to set
	 */
	public void setmFinishedTasks(long mFinishedTasks) {
		this.mFinishedTasks = mFinishedTasks;
	}

	/**
	 * @return the mDeadline
	 */
	public Double getmDeadline() {
		return mDeadline;
	}

	/**
	 * @param mDeadline
	 *            the mDeadline to set
	 */
	public void setmDeadline(Double mDeadline) {
		this.mDeadline = mDeadline;
	}

	/**
	 * @return the mFirstReduceTime
	 */
	public Double getmFirstReduceTime() {
		return mFirstReduceTime;
	}

	/**
	 * @param mFirstReduceTime
	 *            the mFirstReduceTime to set
	 */
	public void setmFirstReduceTime(Double mFirstReduceTime) {
		this.mFirstReduceTime = mFirstReduceTime;
	}

	/**
	 * @return the mStartTime
	 */
	public Double getmStartTime() {
		return mStartTime;
	}

	/**
	 * @param mStartTime
	 *            the mStartTime to set
	 */
	public void setmStartTime(Double mStartTime) {
		this.mStartTime = mStartTime;
	}

	/**
	 * @return the mFinishTime
	 */
	public Double getmFinishTime() {
		return mFinishTime;
	}

	/**
	 * @param mFinishTime
	 *            the mFinishTime to set
	 */
	public void setmFinishTime(Double mFinishTime) {
		this.mFinishTime = mFinishTime;
	}

	/**
	 * @return the mSubmitTime
	 */
	public Double getmSubmitTime() {
		return mSubmitTime;
	}

	/**
	 * @param mSubmitTime
	 *            the mSubmitTime to set
	 */
	public void setmSubmitTime(Double mSubmitTime) {
		this.mSubmitTime = mSubmitTime;
	}

	/**
	 * @return the mName
	 */
	public String getmName() {
		return mName;
	}

	/**
	 * @param mName
	 *            the mName to set
	 */
	public void setmName(String mName) {
		this.mName = mName;
	}

	/**
	 * @return the mNumMapSlots
	 */
	public Long getmNumMapSlots() {
		return mNumMapSlots;
	}

	/**
	 * @param mNumMapSlots
	 *            the mNumMapSlots to set
	 */
	public void setmNumMapSlots(Long mNumMapSlots) {
		this.mNumMapSlots = mNumMapSlots;
	}

	/**
	 * @return the mNumReduceSlots
	 */
	public Long getmNumReduceSlots() {
		return mNumReduceSlots;
	}

	/**
	 * @param mNumReduceSlots
	 *            the mNumReduceSlots to set
	 */
	public void setmNumReduceSlots(Long mNumReduceSlots) {
		this.mNumReduceSlots = mNumReduceSlots;
	}

	/**
	 * @return the mMapTime
	 */
	public Double getmMapTime() {
		return mMapTime;
	}

	/**
	 * @param mMapTime the mMapTime to set
	 */
	public void setmMapTime(Double mMapTime) {
		this.mMapTime = mMapTime;
	}

	/**
	 * @return the mReduceTime
	 */
	public Double getmReduceTime() {
		return mReduceTime;
	}

	/**
	 * @param mReduceTime the mReduceTime to set
	 */
	public void setmReduceTime(Double mReduceTime) {
		this.mReduceTime = mReduceTime;
	}

	/**
	 * @return the mBatchId
	 */
	public long getmBatchId() {
		return mBatchId;
	}

	/**
	 * @param mBatchId the mBatchId to set
	 */
	public void setmBatchId(long mBatchId) {
		this.mBatchId = mBatchId;
	}

	/**
	 * @return the mExpectedFinishTime
	 */
	public Double getmExpectedFinishTime() {
		return mExpectedFinishTime;
	}

	/**
	 * @param mExpectedFinishTime the mExpectedFinishTime to set
	 */
	public void setmExpectedFinishTime(Double mExpectedFinishTime) {
		this.mExpectedFinishTime = mExpectedFinishTime;
	}
}
