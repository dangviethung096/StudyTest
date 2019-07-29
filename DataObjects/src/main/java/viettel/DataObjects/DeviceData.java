package viettel.DataObjects;

public class DeviceData {
	private String device;
	private String deviceType;
	private Double signal;
	private java.sql.Date time;
	
	
	/**
	 * @return the device
	 */
	public String getDevice() {
		return device;
	}
	/**
	 * @param device the device to set
	 */
	public void setDevice(String device) {
		this.device = device;
	}
	/**
	 * @return the deviceType
	 */
	public String getDeviceType() {
		return deviceType;
	}
	/**
	 * @param deviceType the deviceType to set
	 */
	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}
	/**
	 * @return the signal
	 */
	public Double getSignal() {
		return signal;
	}
	/**
	 * @param signal the signal to set
	 */
	public void setSignal(Double signal) {
		this.signal = signal;
	}
	/**
	 * @return the time
	 */
	public java.sql.Date getTime() {
		return time;
	}
	/**
	 * @param time the time to set
	 */
	public void setTime(java.sql.Date time) {
		this.time = time;
	}
}
