package mapreduce.base;

import java.util.ArrayList;
import java.util.List;

public class VirtualMachine {
	long mUserId;
	long mId;
	List<Vm> mMSIdleList;
	List<Vm> mRSIdleList;

	public enum VmType {
		PRORESERVED, RESERVED, ONDEMAND, SPOT
	};

	public enum SlotType {
		MAP, REDUCE,
	};

	Double mBid;
	Double mPrice;
	Double mIncurredCost;
	VmType mType;
	int mNumScheduledTasks;
	Double mAllocationTime;
	Double mTotalAllocatedTime;

	public VirtualMachine(long theUserId, long theId, VmType theType) {
		this.mUserId = theUserId;
		this.mId = theId;
		this.mType = theType;
		this.mMSIdleList = new ArrayList<Vm>();
		this.mRSIdleList = new ArrayList<Vm>();
		mNumScheduledTasks = 0;
		mBid = 0D;
		mPrice = 0D;
		mIncurredCost = 0D;
		mAllocationTime = 0D;
		mTotalAllocatedTime = 0D;
		if(this.mType==VmType.RESERVED) { 
			mIncurredCost = 372.0; //m3.medium reserved instance pricing with no upfront
			mPrice = 372.0;
		}
	}

	public void mSetMSList(List<Vm> theMS) {
		this.mMSIdleList.addAll(theMS);
	}

	public void mSetRSList(List<Vm> theRS) {
		this.mRSIdleList.addAll(theRS);
	}

	/**
	 * @param curtime
	 */
	public void mSetCostIncurred(Double curtime) {
		if(this.mType!=VmType.RESERVED) {
			Double interval = curtime - this.getmAllocationTime();
			Double totalallocationtime = this.getmTotalAllocatedTime() + interval;
			Double costincurred = Math.ceil(interval / 3600.0D) * this.getmPrice();
			this.setmTotalAllocatedTime(totalallocationtime);
			this.setmIncurredCost(this.getmIncurredCost() + costincurred);
		}
		else if(this.mType==VmType.RESERVED) {
			Double interval = curtime - this.getmAllocationTime();
			Double totalallocationtime = this.getmTotalAllocatedTime() + interval;
			this.setmTotalAllocatedTime(totalallocationtime);
		}
	}

	public List<Vm> __getMSList() {
		return this.mMSIdleList;
	}

	public List<Vm> __getRSList() {
		return this.mRSIdleList;
	}

	public long __getId() {
		// TODO Auto-generated method stub
		return this.mId;
	}

	public VmType __getType() {
		return this.mType;
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
	 * @return the mType
	 */
	public VmType getmType() {
		return mType;
	}

	/**
	 * @param mType
	 *            the mType to set
	 */
	public void setmType(VmType mType) {
		this.mType = mType;
	}

	/**
	 * @return the mNumScheduledTasks
	 */
	public int getmNumScheduledTasks() {
		return mNumScheduledTasks;
	}

	/**
	 * @param mNumScheduledTasks
	 *            the mNumScheduledTasks to set
	 */
	public void setmNumScheduledTasks(int mNumScheduledTasks) {
		this.mNumScheduledTasks = mNumScheduledTasks;
	}

	/**
	 * @return the mBid
	 */
	public Double getmBid() {
		return mBid;
	}

	/**
	 * @param mBid
	 *            the mBid to set
	 */
	public void setmBid(Double mBid) {
		this.mBid = mBid;
	}

	/**
	 * @return the mAllocationTime
	 */
	public Double getmAllocationTime() {
		return mAllocationTime;
	}

	/**
	 * @param mAllocationTime
	 *            the mAllocationTime to set
	 */
	public void setmAllocationTime(Double mAllocationTime) {
		this.mAllocationTime = mAllocationTime;
	}

	/**
	 * @return the mTotalAllocatedTime
	 */
	public Double getmTotalAllocatedTime() {
		return mTotalAllocatedTime;
	}

	/**
	 * @param mTotalAllocatedTime
	 *            the mTotalAllocatedTime to set
	 */
	public void setmTotalAllocatedTime(Double mTotalAllocatedTime) {
		this.mTotalAllocatedTime = mTotalAllocatedTime;
	}

	/**
	 * @return the mPrice
	 */
	public Double getmPrice() {
		return mPrice;
	}

	/**
	 * @param mPrice
	 *            the mPrice to set
	 */
	public void setmPrice(Double mPrice) {
		this.mPrice = mPrice;
	}

	/**
	 * @return the mIncurredCost
	 */
	public Double getmIncurredCost() {
		return mIncurredCost;
	}

	/**
	 * @param mIncurredCost
	 *            the mIncurredCost to set
	 */
	public void setmIncurredCost(Double mIncurredCost) {
		this.mIncurredCost = mIncurredCost;
	}
}
