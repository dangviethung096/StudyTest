package viettel.KafkaApp;


public class HeartRateRaw {

	private double heart_rate;

	@Override
	public String toString() {
		return "{ \"heart_rate\" :" + heart_rate + " }";
	}

	public HeartRateRaw() {
	}

	/**
	 * @return the heart_rate
	 */
	public double getHeart_rate() {
		return heart_rate;
	}

	/**
	 * @param heart_rate the heart_rate to set
	 */
	public void setHeart_rate(double heart_rate) {
		this.heart_rate = heart_rate;
	}
}