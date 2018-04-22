package org.keepcoding

import org.keepcoding.batchlayer.MetricasSparkSQL

object BatchApplication {

  def main(args: Array[String]): Unit ={

    if (args.length == 2){
      MetricasSparkSQL.run(args)
    } else {
      println("Se esta intentando arrancar el job de Spark sin los parametros correctos")
    }
  }
}
