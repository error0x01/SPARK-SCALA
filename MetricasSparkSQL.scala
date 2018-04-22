package org.keepcoding.batchlayer

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.keepcoding.dominio.{Cliente, Geolocalizacion, Transaccion}

object MetricasSparkSQL {

  def run(args: Array[String]): Unit ={

    val sparkSession = SparkSession.builder().master("local[*]")
          .appName(name="Practica final -Batch layer - Spark SQL")
          .getOrCreate()

    import sparkSession.implicits._

    val rddTransacciones = sparkSession.read.csv(path = s"file:///${args(0)}")

    val cabecera = rddTransacciones.first()

    val rddSinCabecera = rddTransacciones.filter(!_.equals(cabecera)).map(_.toString().split(","))

    val acumulador: LongAccumulator = sparkSession.sparkContext.longAccumulator(name = "dniCliente")
    val acumuladorTransaccion: LongAccumulator = sparkSession.sparkContext.longAccumulator(name = "dniTransaccion")

    /*
    Cliente(DNI: Long, nombre: String, cuentaCorriente: String)
     */

    val dfClientes = rddSinCabecera.map(columna =>  {
      acumulador.add(acumulador.value +1)
      Cliente(acumulador.value, columna(4), columna(6))
    })

    dfClientes.show( numRows = 10)

    /*
    Transaccion(DNI:Long, importe:Double, descripcion:String, categoria:String, tarjetaCredito:String,
                geolocalizacion:Geolocalizacion)
     */

    val dfTransacciones = rddSinCabecera.map(columna => {

      acumuladorTransaccion.add(acumulador.value +1)
      Transaccion(acumuladorTransaccion.value, columna(2).toDouble, columna(10), "N/A", columna(3),
            Geolocalizacion(columna(8).toDouble, columna(9).toDouble, columna(5), "N/A"))

    })

    dfTransacciones.show(numRows = 10)

    dfTransacciones.createGlobalTempView(viewName = "TRANSACCIONES")

    val dfOutput = sparkSession.sql(sqlText = "SELECT * FROM global_temp.TRANSACCIONES")

    dfOutput.show(numRows = 10)

    // dfOutput.write.csv(path = args(1))
  }
}
