package viettel.Spark.Cassandra;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import viettel.DataObjects.HeartRate;
import viettel.DataObjects.HeartRateAvg;

public class Test {
	public static void main(String[] args) throws Exception {
		// connect
		CassandraAPI.getInstance().connect();
		
		Random random = new Random();
		HeartRate heartRate = new HeartRate();
		heartRate.setCurrentHeartRate(100*random.nextDouble());
//		heartRate.setId(new UUID(random.nextLong(), random.nextLong()));
		heartRate.setTime(new Timestamp(new Date().getTime()));
		
		CassandraAPI.getInstance().insertToDB(heartRate);
		// close
		CassandraAPI.getInstance().close();
	}
}
