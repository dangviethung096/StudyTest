package viettel.Spark.DataObjects;

import java.sql.Timestamp;
import java.util.UUID;

public class HeartRate {
	
	private UUID id;
	private Timestamp time;
	private double current_heart_rate;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public Timestamp getTime() {
		return time;
	}

	public void setTime(Timestamp time) {
		this.time = time;
	}

	public double getCurrent_heart_rate() {
		return current_heart_rate;
	}

	public void setCurrent_heart_rate(double current_heart_rate) {
		this.current_heart_rate = current_heart_rate;
	}

	public HeartRate() {
	}
}
