package org.keepcoding

import org.keepcoding.speedlayer.MetricasSparkStreaming

object StreamingApplication {

  def main(args: Array[String]): Unit ={

    if (args.length == 1){
      MetricasSparkStreaming.run(args)
    } else {
      println("Se esta intentando arrancar el job de Spark sin los parametros correctos")
    }
  }

}


