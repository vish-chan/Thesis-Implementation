package mapreduce.manager;

import java.util.ArrayList;
import java.util.List;

import mapreduce.base.VirtualMachine;
import mapreduce.base.VirtualMachine.VmType;
import sun.rmi.runtime.Log;

public class VMPoolManager {
	List<VirtualMachine> mReservedVMPoolList;
	List<VirtualMachine> mProductionVMPoolList;
	List<VirtualMachine> mOnDemandVMPoolList;
	List<VirtualMachine> mSpotVMPoolList;
	
	public VMPoolManager() {
		mProductionVMPoolList = new ArrayList<VirtualMachine>();
		mReservedVMPoolList = new ArrayList<VirtualMachine>();
		mOnDemandVMPoolList = new ArrayList<VirtualMachine>();
		mSpotVMPoolList = new ArrayList<VirtualMachine>();
	}
	
	public void mAddToVMPool(VirtualMachine theVm) {
		if(theVm.__getType()==VmType.PRORESERVED)
			this.mProductionVMPoolList.add(theVm);
		else if(theVm.__getType()==VmType.RESERVED)
			this.mReservedVMPoolList.add(theVm);
		else if(theVm.__getType()==VmType.ONDEMAND)
			this.mOnDemandVMPoolList.add(theVm);
		else if(theVm.__getType()==VmType.SPOT)
			this.mSpotVMPoolList.add(theVm);
	}
	
	public VirtualMachine mAllocateVMFromPool(VmType mType) {
		VirtualMachine vm = null;
		if(mType==VmType.PRORESERVED) {
			if(mProductionVMPoolList.size()>0) {
				vm = mProductionVMPoolList.get(0);
				mProductionVMPoolList.remove(vm);
			}
		}
		
		else if(mType==VmType.RESERVED) {
			if(mReservedVMPoolList.size()>0) {
				vm = mReservedVMPoolList.get(0);
				mReservedVMPoolList.remove(vm);
			}
		}
		else if(mType==VmType.ONDEMAND){
			if(mOnDemandVMPoolList.size()>0) {
				vm = mOnDemandVMPoolList.get(0);
				mOnDemandVMPoolList.remove(vm);
			}
		}
		else if(mType==VmType.SPOT){
			if(mSpotVMPoolList.size()>0) {
				vm = mSpotVMPoolList.get(0);
				mSpotVMPoolList.remove(vm);
			}
		}
		return vm;
	}
	
	public void mDeallocateVMToPool(VirtualMachine vm) {
		mAddToVMPool(vm);
		Log.printLine(CloudSim.clock()+": Deallocated VM #"+vm.__getId()+" to pool\n");
	}

	/**
	 * @return the mReservedVMPoolList
	 */
	public List<VirtualMachine> getmReservedVMPoolList() {
		return mReservedVMPoolList;
	}

	/**
	 * @param mReservedVMPoolList the mReservedVMPoolList to set
	 */
	public void setmReservedVMPoolList(List<VirtualMachine> mReservedVMPoolList) {
		this.mReservedVMPoolList = mReservedVMPoolList;
	}

	/**
	 * @return the mOnDemandVMPoolList
	 */
	public List<VirtualMachine> getmOnDemandVMPoolList() {
		return mOnDemandVMPoolList;
	}

	/**
	 * @param mOnDemandVMPoolList the mOnDemandVMPoolList to set
	 */
	public void setmOnDemandVMPoolList(List<VirtualMachine> mOnDemandVMPoolList) {
		this.mOnDemandVMPoolList = mOnDemandVMPoolList;
	}

	/**
	 * @return the mSpotVMPoolList
	 */
	public List<VirtualMachine> getmSpotVMPoolList() {
		return mSpotVMPoolList;
	}

	/**
	 * @param mSpotVMPoolList the mSpotVMPoolList to set
	 */
	public void setmSpotVMPoolList(List<VirtualMachine> mSpotVMPoolList) {
		this.mSpotVMPoolList = mSpotVMPoolList;
	}

	/**
	 * @return the mProductionVMPoolList
	 */
	public List<VirtualMachine> getmProductionVMPoolList() {
		return mProductionVMPoolList;
	}

	/**
	 * @param mProductionVMPoolList the mProductionVMPoolList to set
	 */
	public void setmProductionVMPoolList(List<VirtualMachine> mProductionVMPoolList) {
		this.mProductionVMPoolList = mProductionVMPoolList;
	}
}
