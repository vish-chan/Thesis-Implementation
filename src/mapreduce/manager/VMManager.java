package mapreduce.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import mapreduce.base.VirtualMachine;
import mapreduce.base.VirtualMachine.SlotType;
import mapreduce.base.VirtualMachine.VmType;
import sun.rmi.runtime.Log;

public class VMManager {

	VMPoolManager mVMPoolManager;
	VMPriceManager mVMPriceManager;

	public enum Strategy {
		Hybrid, OnDemand, Spot,
	};

	Strategy mStrategy;
	/*---Data structures for user jobs---*/
	List<VirtualMachine> mRunningVMList;
	List<Integer> mRunningMSlotsList;
	List<Integer> mIdleMSlotsList;
	List<Integer> mRunningRSlotsList;
	List<Integer> mIdleRSlotsList;
	HashMap<Long, Long> mSlotVMIdMap = new HashMap<Long, Long>();
	HashMap<Long, VmType> mSlotVMTypeMap = new HashMap<Long, VmType>();
	HashMap<Long, Double> mSlotBidMap = new HashMap<Long, Double>();
	HashMap<Integer, SlotType> mSlotTypeMap = new HashMap<Integer, VirtualMachine.SlotType>();

	/*---Data structures for production jobs---*/
	List<VirtualMachine> mProductionVMList;
	List<Integer> mProductionMSlotsList;
	List<Integer> mProductionRSlotsList;

	public VMManager(VMPoolManager theVMP, String theStrategy) {
		this.mVMPoolManager = theVMP;
		if (theStrategy.equals("1"))
			this.mStrategy = Strategy.Hybrid;
		else if (theStrategy.equals("2"))
			this.mStrategy = Strategy.OnDemand;
		else
			this.mStrategy = Strategy.Spot;
		this.mVMPriceManager = new VMPriceManager();
		this.mRunningVMList = new ArrayList<VirtualMachine>();
		this.mRunningMSlotsList = new ArrayList<Integer>();
		this.mRunningRSlotsList = new ArrayList<Integer>();
		this.mIdleMSlotsList = new ArrayList<Integer>();
		this.mIdleRSlotsList = new ArrayList<Integer>();
		this.mProductionVMList = new ArrayList<VirtualMachine>();
		this.mProductionMSlotsList = new ArrayList<Integer>();
		this.mProductionRSlotsList = new ArrayList<Integer>();
	}

	public void mInitFixedPool() {
		long size = mVMPoolManager.getmReservedVMPoolList().size();
		VirtualMachine vm = null;
		for (int i = 0; i < size; i++) {
			vm = mVMPoolManager.mAllocateVMFromPool(VmType.RESERVED);
			mRunningVMList.add(vm);
			mAddSlotsToMap(vm);
			Log.print("VM#" + vm.__getId() + " of type " + VmType.RESERVED
					+ " added to running user vm list\n");
		}
		size = mVMPoolManager.getmProductionVMPoolList().size();
		for (int i = 0; i < size; i++) {
			vm = mVMPoolManager.mAllocateVMFromPool(VmType.PRORESERVED);
			mProductionVMList.add(vm);
			mProduction_AddSlotsToMap(vm);
			Log.print("VM#" + vm.__getId() + " of type " + vm.getmType()
					+ " added to running production vm list\n");
		}
	}

	public void mDeInitFixedPool() {
		List<VirtualMachine> mTempVMList = new ArrayList<VirtualMachine>();
		mTempVMList.addAll(mRunningVMList);
		for (VirtualMachine vm : mTempVMList)
			mDeallocateVMToPool(vm);
		mTempVMList.clear();
		mTempVMList.addAll(mProductionVMList);
		for (VirtualMachine vm : mTempVMList)
			mProduction_DeallocateVMToPool(vm);
	}

	public void mAddSlotsToMap(VirtualMachine theVm) {
		for (Vm slot : theVm.__getMSList()) {
			mSlotVMIdMap.put(new Long(slot.getId()), new Long(theVm.__getId()));
			mSlotVMTypeMap.put(new Long(slot.getId()), theVm.__getType());
			mSlotBidMap.put(new Long(slot.getId()), theVm.getmBid());
			mSlotTypeMap.put(new Integer(slot.getId()), SlotType.MAP);
			mIdleMSlotsList.add(slot.getId());
			Log.print("Map slot " + slot.getId() + " added to map for VM#"
					+ theVm.__getId() + "\n");
		}

		for (Vm slot : theVm.__getRSList()) {
			mSlotVMIdMap.put(new Long(slot.getId()), new Long(theVm.__getId()));
			mSlotVMTypeMap.put(new Long(slot.getId()), theVm.__getType());
			mSlotBidMap.put(new Long(slot.getId()), theVm.getmBid());
			mSlotTypeMap.put(new Integer(slot.getId()), SlotType.REDUCE);
			mIdleRSlotsList.add(slot.getId());
			Log.print("Reduce slot " + slot.getId() + " added to map for VM#"
					+ theVm.__getId() + "\n");
		}
	}

	public void mProduction_AddSlotsToMap(VirtualMachine theVm) {
		for (Vm slot : theVm.__getMSList()) {
			mProductionMSlotsList.add(slot.getId());
			mSlotVMIdMap.put(new Long(slot.getId()), theVm.__getId());
			mSlotVMTypeMap.put(new Long(slot.getId()), theVm.__getType());
			mSlotBidMap.put(new Long(slot.getId()), theVm.getmBid());
			mSlotTypeMap.put(new Integer(slot.getId()), SlotType.MAP);
			Log.print("Map slot " + slot.getId() + " added to map for VM#"
					+ theVm.__getId() + "\n");
		}
		for (Vm slot : theVm.__getRSList()) {
			mProductionRSlotsList.add(slot.getId());
			mSlotVMIdMap.put(new Long(slot.getId()), theVm.__getId());
			mSlotVMTypeMap.put(new Long(slot.getId()), theVm.__getType());
			mSlotBidMap.put(new Long(slot.getId()), theVm.getmBid());
			mSlotTypeMap.put(new Integer(slot.getId()), SlotType.REDUCE);
			Log.print("Reduce slot " + slot.getId() + " added to map for VM#"
					+ theVm.__getId() + "\n");
		}
	}

	public Double mAllocateVMFromPool(Double theEndtime) {
		VirtualMachine vm = null;
		VmType type = null;
		Double highestbid = mVMPriceManager.mPredictNextHighestBid(theEndtime);
		if (mStrategy == Strategy.Hybrid)
			type = mVMPriceManager.mPreferredVMType(highestbid, 1);
		else if (mStrategy == Strategy.OnDemand)
			type = VmType.ONDEMAND;
		else
			type = VmType.SPOT;
		vm = mVMPoolManager.mAllocateVMFromPool(type);
		if (vm != null) {
			vm.setmAllocationTime(CloudSim.clock());
			if (type == VmType.SPOT) {
				vm.setmBid(highestbid);
			} else if (type == VmType.ONDEMAND){
				vm.setmPrice(mVMPriceManager.getmOnDemandInstancePrice());
			}
			mRunningVMList.add(vm);
			mAddSlotsToMap(vm);
			Log.print("VM#" + vm.__getId() + " of type " + type
					+ " added to running vm list\n");
		} else {
			Log.print("Couldnot assign more VMs, Please increase pool size!\n");
			CloudSim.stopSimulation();
		}
		return vm.getmBid();
	}

	public void mRemoveSlotsFromMap(VirtualMachine theVm) {
		for (Vm slot : theVm.__getMSList()) {
			mSlotVMIdMap.remove(new Long(slot.getId()));
			mSlotVMTypeMap.remove(new Long(slot.getId()));
			mSlotBidMap.remove(new Long(slot.getId()));
			mSlotTypeMap.remove(new Integer(slot.getId()));
			mIdleMSlotsList.remove(new Integer(slot.getId()));
			mProductionMSlotsList.remove(new Integer(slot.getId()));
			Log.print("Map slot " + slot.getId() + " removed from map for VM#"
					+ theVm.__getId() + "\n");
		}

		for (Vm slot : theVm.__getRSList()) {
			mSlotVMIdMap.remove(new Long(slot.getId()));
			mSlotVMTypeMap.remove(new Long(slot.getId()));
			mSlotBidMap.remove(new Long(slot.getId()));
			mSlotTypeMap.remove(new Integer(slot.getId()));
			mIdleRSlotsList.remove(new Integer(slot.getId()));
			mProductionRSlotsList.remove(new Integer(slot.getId()));
			Log.print("Reduce slot " + slot.getId()
					+ " removed from map for VM#" + theVm.__getId() + "\n");
		}
	}

	public void mCheckVMListForDeallocation() {
		List<VirtualMachine> mTempVMList = new ArrayList<VirtualMachine>();
		for (VirtualMachine vm : mRunningVMList)
			if (vm.getmNumScheduledTasks() == 0
					&& vm.getmType() != VmType.RESERVED)
				mTempVMList.add(vm);
		for (VirtualMachine vm : mTempVMList)
			mDeallocateVMToPool(vm);
	}

	public void mDeallocateVMToPool(VirtualMachine theVm) {
		int i = -1;
		for (VirtualMachine vm : mRunningVMList) {
			if (vm.__getId() == theVm.__getId()) {
				i = mRunningVMList.indexOf(vm);
				break;
			}
		}
		mRunningVMList.remove(i);
		mRemoveSlotsFromMap(theVm);
		if(theVm.getmType()==VmType.SPOT) {
			theVm.setmPrice(mVMPriceManager.mCurrentSpotPrice(CloudSim.clock()));
		}
		theVm.mSetCostIncurred(CloudSim.clock());
		mVMPoolManager.mDeallocateVMToPool(theVm);
		Log.printLine(CloudSim.clock() + ": VM #" + theVm.__getId()
				+ " sent to pool\n");
	}

	public void mProduction_DeallocateVMToPool(VirtualMachine theVm) {
		int i = -1;
		for (VirtualMachine vm : mProductionVMList) {
			if (vm.__getId() == theVm.__getId()) {
				i = mProductionVMList.indexOf(vm);
				break;
			}
		}
		mProductionVMList.remove(i);
		mRemoveSlotsFromMap(theVm);
		theVm.mSetCostIncurred(CloudSim.clock());
		mVMPoolManager.mDeallocateVMToPool(theVm);
		Log.printLine(CloudSim.clock() + ": VM #" + theVm.__getId()
				+ " sent to pool\n");
	}

	public void mAddMapSlotToRunningList(int slotid) {
		if (!this.mRunningMSlotsList.contains(new Integer(slotid))) {
			this.mRunningMSlotsList.add(new Integer(slotid));
			this.mIdleMSlotsList.remove(new Integer(slotid));
		}
	}

	public void mAddReduceSlotToRunningList(int slotid) {
		if (!this.mRunningRSlotsList.contains(new Integer(slotid))) {
			this.mRunningRSlotsList.add(new Integer(slotid));
			this.mIdleRSlotsList.remove(new Integer(slotid));
		}
	}

	public void mRemoveMapSlotFromRunningList(int slotid) {
		this.mRunningMSlotsList.remove(new Integer(slotid));
		this.mIdleMSlotsList.add(new Integer(slotid));
	}

	public void mRemoveReduceSlotFromRunningList(int slotid) {
		this.mRunningRSlotsList.remove(new Integer(slotid));
		this.mIdleRSlotsList.add(new Integer(slotid));
	}

	public void mChangeScheduledTaskCount(int slotid, int change) {
		Long vmid = mSlotVMIdMap.get(new Long(slotid));
		for (VirtualMachine vm : mRunningVMList) {
			if (vm.__getId() == vmid) {
				int i = mRunningVMList.indexOf(vm);
				vm.setmNumScheduledTasks(vm.getmNumScheduledTasks() + change);
				mRunningVMList.set(i, vm);
				break;
			}
		}
	}

	public void mProduction_ChangeScheduledTaskCount(int slotid, int change) {
		Long vmid = mSlotVMIdMap.get(new Long(slotid));
		for (VirtualMachine vm : mProductionVMList) {
			if (vm.__getId() == vmid) {
				int i = mProductionVMList.indexOf(vm);
				vm.setmNumScheduledTasks(vm.getmNumScheduledTasks() + change);
				mProductionVMList.set(i, vm);
				break;
			}
		}
	}

	public SlotType getmSlotType(Integer slotid) {
		return mSlotTypeMap.get(slotid);
	}

	public VmType getmSlotVmType(Integer slotid) {
		return mSlotVMTypeMap.get(slotid);
	}

	public Double getmSlotBid(Integer slotid) {
		return mSlotBidMap.get(slotid);
	}

	public Double getmSpotBid(Double endtime) {
		return this.mVMPriceManager.mPredictNextHighestBid(endtime);
	}

	/**
	 * @return the mRunningMSlotsList
	 */
	public List<Integer> getmRunningMSlotsList() {
		return mRunningMSlotsList;
	}

	/**
	 * @param mRunningMSlotsList
	 *            the mRunningMSlotsList to set
	 */
	public void setmRunningMSlotsList(List<Integer> mRunningMSlotsList) {
		this.mRunningMSlotsList = mRunningMSlotsList;
	}

	/**
	 * @return the mIdleMSlotsList
	 */
	public List<Integer> getmIdleMSlotsList() {
		return mIdleMSlotsList;
	}

	/**
	 * @param mIdleMSlotsList
	 *            the mIdleMSlotsList to set
	 */
	public void setmIdleMSlotsList(List<Integer> mIdleMSlotsList) {
		this.mIdleMSlotsList = mIdleMSlotsList;
	}

	/**
	 * @return the mRunningRSlotsList
	 */
	public List<Integer> getmRunningRSlotsList() {
		return mRunningRSlotsList;
	}

	/**
	 * @param mRunningRSlotsList
	 *            the mRunningRSlotsList to set
	 */
	public void setmRunningRSlotsList(List<Integer> mRunningRSlotsList) {
		this.mRunningRSlotsList = mRunningRSlotsList;
	}

	/**
	 * @return the mIdleRSlotsList
	 */
	public List<Integer> getmIdleRSlotsList() {
		return mIdleRSlotsList;
	}

	/**
	 * @param mIdleRSlotsList
	 *            the mIdleRSlotsList to set
	 */
	public void setmIdleRSlotsList(List<Integer> mIdleRSlotsList) {
		this.mIdleRSlotsList = mIdleRSlotsList;
	}

	/**
	 * @return the mProductionVMList
	 */
	public List<VirtualMachine> getmProductionVMList() {
		return mProductionVMList;
	}

	/**
	 * @param mProductionVMList
	 *            the mProductionVMList to set
	 */
	public void setmProductionVMList(List<VirtualMachine> mProductionVMList) {
		this.mProductionVMList = mProductionVMList;
	}

	/**
	 * @return the mProductionMapSlotsList
	 */

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
	 * @return the mRunningVMList
	 */
	public List<VirtualMachine> getmRunningVMList() {
		return mRunningVMList;
	}

	/**
	 * @param mRunningVMList
	 *            the mRunningVMList to set
	 */
	public void setmRunningVMList(List<VirtualMachine> mRunningVMList) {
		this.mRunningVMList = mRunningVMList;
	}

	/**
	 * @return the mProductionMSlotsList
	 */
	public List<Integer> getmProductionMSlotsList() {
		return mProductionMSlotsList;
	}

	/**
	 * @param mProductionMSlotsList
	 *            the mProductionMSlotsList to set
	 */
	public void setmProductionMSlotsList(List<Integer> mProductionMSlotsList) {
		this.mProductionMSlotsList = mProductionMSlotsList;
	}

	/**
	 * @return the mProductionRSlotsList
	 */
	public List<Integer> getmProductionRSlotsList() {
		return mProductionRSlotsList;
	}

	/**
	 * @param mProductionRSlotsList
	 *            the mProductionRSlotsList to set
	 */
	public void setmProductionRSlotsList(List<Integer> mProductionRSlotsList) {
		this.mProductionRSlotsList = mProductionRSlotsList;
	}

	/**
	 * @return the mVMPriceManager
	 */
	public VMPriceManager getmVMPriceManager() {
		return mVMPriceManager;
	}

	/**
	 * @param mVMPriceManager the mVMPriceManager to set
	 */
	public void setmVMPriceManager(VMPriceManager mVMPriceManager) {
		this.mVMPriceManager = mVMPriceManager;
	}

	/**
	 * @return the mStrategy
	 */
	public Strategy getmStrategy() {
		return mStrategy;
	}

	/**
	 * @param mStrategy the mStrategy to set
	 */
	public void setmStrategy(Strategy mStrategy) {
		this.mStrategy = mStrategy;
	}
}
