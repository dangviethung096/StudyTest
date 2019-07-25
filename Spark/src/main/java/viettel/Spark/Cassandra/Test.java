package viettel.Spark.Cassandra;

import java.sql.Timestamp;
import java.util.UUID;

import viettel.Spark.DataObjects.HeartRateAvg;

public class Test {
	public static void main(String[] args) {
		// connect
		CassandraAPI.connect();
		
		HeartRateAvg heartRateAvg = new HeartRateAvg();
		heartRateAvg.setDay(new Timestamp(123456L));
		heartRateAvg.setHeart_rate_avg(12.1);
		heartRateAvg.setHeart_rate_max(15.3);
		heartRateAvg.setHeart_rate_min(9.5);
		heartRateAvg.setId(new UUID(12L, 33L));
		
		CassandraAPI.getInstance().insertToDB(heartRateAvg);
		// close
		CassandraAPI.close();
	}
}
