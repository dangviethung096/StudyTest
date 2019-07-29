package viettel.DataObjects;

import java.sql.Timestamp;
import java.util.UUID;

public class HeartRate {
	
	private String id;
	private Timestamp time;
	private double currentHeartRate;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Timestamp getTime() {
		return time;
	}

	public void setTime(Timestamp time) {
		this.time = time;
	}

	public double getCurrentHeartRate() {
		return currentHeartRate;
	}

	public void setCurrentHeartRate(double currentHeartRate) {
		this.currentHeartRate = currentHeartRate;
	}
	
	public String toString() {
		return "{ id : " + id  + ", time : " + time.getTime() + ", current_heart_rate : " + currentHeartRate + " }";  
	}
	
	public HeartRate() {
	}
}
