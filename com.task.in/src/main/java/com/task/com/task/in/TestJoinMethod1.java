package com.task.com.task.in;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Hello world!
 *
 */
class TestJoinMethod1 extends Thread implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static SparkSession spark;
	static Dataset<Row> AllCsvData;
	static Set<String> key = new HashSet<String>();
	static List<String> listkey = new ArrayList<String>();
	static Dataset<Row> prin;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static Properties props = null;

	public void run() {
		prin.coalesce(1).foreach(DataFromCsv -> {
			new TestJoinMethod1().MakeJsonObjectAndReturnItAsString(DataFromCsv.getAs("DeviceID"),
					DataFromCsv.getAs("DataTime"), DataFromCsv.getAs("CurrentA"), DataFromCsv.getAs("CurrentB"));
		});
		prin.show();
	}

	public void MakeJsonObjectAndReturnItAsString(String DeviceId, String DataTime, String CurrentA, String CurrentB)
			throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		JsonObject finalJson = new JsonObject();
		finalJson.addProperty("deviceId", Integer.valueOf(DeviceId));
		finalJson.addProperty("dataTime", sdf.parse(DataTime).getTime());
		JsonObject jsonA = new JsonObject();
		jsonA.addProperty("dataName", "CurrentA");
		jsonA.addProperty("dataValue", Float.valueOf(CurrentA));
		JsonObject jsonB = new JsonObject();
		jsonB.addProperty("dataName", "CurrentB");
		jsonB.addProperty("dataValue", Float.valueOf(CurrentB));
		JsonArray array = new JsonArray();
		array.add(jsonA);
		array.add(jsonB);

		finalJson.add("dataList", array);
		ImportJsonDataIntoKafka(finalJson.toString());
	}

	public void ImportJsonDataIntoKafka(String finalData) {
		if (props == null) {
			props = new Properties();
			props.put("bootstrap.servers", "localhost:9092");
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		}
		Producer<String, String> producer = new KafkaProducer<>(props);
		producer.send(new ProducerRecord<String, String>("sample-data", finalData, finalData));
		producer.close();
		System.out.println("message:  " + finalData + "  has sent to Topic:  sample-data");

	}

	public static void main(String args[]) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		spark = SparkSession.builder().appName("invitasTastToComplete").master("local[*]").getOrCreate();
		AllCsvData = spark.read().format("com.databricks.spark.csv").option("header", true).option("delimiter", ",")
				.load("src/main/java/SampleData_.csv");
		AllCsvData.foreach(k -> {
			key.add(k.getAs("DeviceID"));
		});
		for (String fill : key) {
			listkey.add(fill);
		}
		for (int i = 0; i < listkey.size(); i++) {
			AllCsvData.createOrReplaceTempView("FireIncidentsSF");
			prin = AllCsvData.sqlContext().sql("select * from FireIncidentsSF where DeviceID=" + listkey.get(i));
			TestJoinMethod1 t1 = new TestJoinMethod1();
			t1.start();
		}

	}

}
