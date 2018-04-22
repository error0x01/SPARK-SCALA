package org.keepcoding.speedlayer

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.keepcoding.dominio.{Cliente, Geolocalizacion, Transaccion}


object MetricasSparkStreaming {

  def run(args: Array[String]): Unit ={

    // Definicion dela configuracion de Spark
    val conf = new SparkConf().setMaster("local[*]").setAppName("Practica final - Speed layer")

    // Definicion de la configuracion de Kafka, servidor, deserializador, serializador, etc
    val KafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-demo",
      "kafka.consumer.id" -> "kafka-consumer-01")

    // Definicion del contexto de Spark Streaming
    val ssc = new StreamingContext(conf, Seconds(1))

    // Definicion del objeto DStream que va a consumir la informacion donde "TransPrac" seria args(0) es el topico
    val inputEnBruto: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      PreferConsistent, Subscribe[String, String](Array("TransPrac"), KafkaParams))

    // guarda el valor consumido de kaffka - aqui es donde se harian las transformaciones de la prectica
    val transaccionesStream: DStream[String] = inputEnBruto.map(_.value())

    // Test para verificar que se está procesando la info introducida por en la shell del primer producer
    transaccionesStream.print()

    // Divido el Stream en función de las "," y devuelvo un Array[String] con el resultado
    val transaccionSplit = transaccionesStream.map(_.toString().split(","))

    // Test - Mando al consumer de Kafka la lista de strings para confirmar que funciona
    transaccionSplit.foreachRDD(rdd => {
      rdd.foreachPartition(writeToKafkaArrStr(args(0)))
    })

    // Producer que recibe un Array[String] y escribe en el consumer de kafka - VALIDADO
    def writeToKafkaArrStr (outputTopic1: String)(partitionOfRecords: Iterator[Array[String]]): Unit = {
      val producer = new KafkaProducer[String, String](getKafkaConfig())
      partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic1, data.toList.toString())))
      producer.flush()
      producer.close()
    }

    // Tupla con la información de Cliente y Transacción
    val streamEnTuplas = transaccionSplit.map(event => {
        new Tuple2(Cliente(scala.util.Random.nextInt(), event(4), event(6)),
              Transaccion(scala.util.Random.nextInt(), event(2).toDouble, event(10), "N/A", event(3),
              Geolocalizacion(event(8).toDouble, event(9).toDouble, event(5), "N/A")))
    })

    // Test - Mando al consumer de Kafka la lista de tuplas para confirmar que funciona
    streamEnTuplas.foreachRDD(rdd => {
      rdd.foreachPartition(writeToKafkaTupla(args(0)))
    })

    // Producer que recibe un (Cliente, Transaccion) y escribe en el consumer de kafka - VALIDADO
    def writeToKafkaTupla (outputTopic1: String)(partitionOfRecords: Iterator[(Cliente, Transaccion)]): Unit = {
      val producer = new KafkaProducer[String, String](getKafkaConfig())
      partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic1, data.toString())))
      producer.flush()
      producer.close()
    }

    // Test - SELECT sobre Clientes

    streamEnTuplas.foreachRDD(foreachFunc = rdd => {

      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val clientesDF = rdd.map(tupla => tupla._1).toDF()

      // Creates a temporary view using the DataFrame
      clientesDF.createOrReplaceTempView("TRANSACCIONES")

      val clientesSelect = spark.sql("SELECT * " + "  FROM TRANSACCIONES")

      // Imprimo por la consola de log
      clientesSelect.show()

    })

    // ¿Cómo enviar el resultado de la select al consumidor de Kafka?
    /*
    clientesPorCiudad.(rdd => {
      rdd.foreachPartition(writeToKafka(args(0)))
      })
    */

    def writeToKafka (outputTopic1: String)(partitionOfRecords: Iterator[Array[String]]): Unit = {
      val producer = new KafkaProducer[String, String](getKafkaConfig())
      partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic1, data.toString())))
      producer.flush()
      producer.close()
    }

    // arranque de treaming y la espera de que finalice el propio job
    ssc.start()
    ssc.awaitTermination()

  }

  // definicion de la configuracion del topico de salida
  def getKafkaConfig(): Properties = {

    val prop = new Properties()

    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    prop

  }
}
