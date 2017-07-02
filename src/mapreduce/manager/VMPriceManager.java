package mapreduce.manager;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mapreduce.base.VirtualMachine.VmType;
import sun.rmi.runtime.Log;

public class VMPriceManager {
	
	static String NAME  = "VmPriceManager";
	Integer mOffset = 0;
	Double mOnDemandInstancePrice;
	Double mReservedInstancePrice;
	Double mLastPredictedSpotPrice;
	Double mLastTimeofPrediction;
	String SPOTPRICEFILENAME = "resources\\spot_instance_true.txt";
	List<Double> mSIPredictedPriceList;
	
	public VMPriceManager() {
		Random r = new Random((long)CloudSim.clock());
		mReservedInstancePrice = new Double(372.0D);
		mOnDemandInstancePrice = new Double(0.07);
		mSIPredictedPriceList = new ArrayList<Double>();
		int val = mInit(SPOTPRICEFILENAME);
		mOffset = r.nextInt(5015);
		Log.printLine(NAME+": SI price manager returns with: "+val +" and offset: "+mOffset);
	}
	
	private int mInit(String filepath) {
		int val = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filepath));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return -2;
		}
		String currentline = new String();
		try {
			while((currentline = br.readLine()) != null)
				mSIPredictedPriceList.add(new Double(Double.parseDouble(currentline)));
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		val = mSIPredictedPriceList.size();
		return val;
	}
	
	public Double mCurrentSpotPrice(Double time) {
		Double time_min = time/60;
		Integer idx = mOffset+(int)Math.ceil(time_min/60);
		return mSIPredictedPriceList.get(Math.min(idx, mSIPredictedPriceList.size()-1));
	}
	
	public Double mPredictNextHighestBid(Double time) {
		Double cur_time_min = CloudSim.clock()/60;
		Integer initial_idx = mOffset+(int)Math.ceil(cur_time_min/60);
		Double end_time_min = time/60;
		Integer final_idx = mOffset+(int)Math.ceil(end_time_min/60);
		Double highestbid = mSIPredictedPriceList.get(initial_idx);
		for(int i=initial_idx+1;i<=Math.min(final_idx, mSIPredictedPriceList.size()-1);i++) {
			if(highestbid < mSIPredictedPriceList.get(i)) {
				highestbid = mSIPredictedPriceList.get(i);
			}
		}
		Log.printLine(CloudSim.clock()+": "+NAME+": Predicting highest bid:"+initial_idx+"|"+final_idx+"|"+highestbid);
		return highestbid;
	}
	
	public VmType mPreferredVMType(Double time) {
		Double spotbidprice = mPredictNextHighestBid(time);
		if(spotbidprice < mOnDemandInstancePrice)
			return VmType.SPOT;
		return VmType.ONDEMAND;
	}
	
	public VmType mPreferredVMType(Double bid, int overload) {
		Double spotbidprice = bid;
		if(spotbidprice < mOnDemandInstancePrice)
			return VmType.SPOT;
		return VmType.ONDEMAND;
	}

	/**
	 * @return the mOnDemandInstancePrice
	 */
	public Double getmOnDemandInstancePrice() {
		return mOnDemandInstancePrice;
	}

	/**
	 * @param mOnDemandInstancePrice the mOnDemandInstancePrice to set
	 */
	public void setmOnDemandInstancePrice(Double mOnDemandInstancePrice) {
		this.mOnDemandInstancePrice = mOnDemandInstancePrice;
	}

	/**
	 * @return the mLastPredictedSpotPrice
	 */
	public Double getmLastPredictedSpotPrice() {
		return mLastPredictedSpotPrice;
	}

	/**
	 * @param mLastPredictedSpotPrice the mLastPredictedSpotPrice to set
	 */
	public void setmLastPredictedSpotPrice(Double mLastPredictedSpotPrice) {
		this.mLastPredictedSpotPrice = mLastPredictedSpotPrice;
	}

	/**
	 * @return the mLastTimeofPrediction
	 */
	public Double getmLastTimeofPrediction() {
		return mLastTimeofPrediction;
	}

	/**
	 * @param mLastTimeofPrediction the mLastTimeofPrediction to set
	 */
	public void setmLastTimeofPrediction(Double mLastTimeofPrediction) {
		this.mLastTimeofPrediction = mLastTimeofPrediction;
	}

	/**
	 * @return the mSIPredictedPriceList
	 */
	public List<Double> getmSIPredictedPriceList() {
		return mSIPredictedPriceList;
	}

	/**
	 * @param mSIPredictedPriceList the mSIPredictedPriceList to set
	 */
	public void setmSIPredictedPriceList(List<Double> mSIPredictedPriceList) {
		this.mSIPredictedPriceList = mSIPredictedPriceList;
	}
}
