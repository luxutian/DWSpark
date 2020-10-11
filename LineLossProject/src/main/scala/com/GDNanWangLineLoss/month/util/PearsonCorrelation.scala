package com.GDNanWangLineLoss.month.util

/**
  * 工具类
  */
object PearsonCorrelation extends Serializable{

    def getPearsonCorrelationScore(col1: String, col2: String): Double = {
        if (col1 != null && col2 != null) {
            val col1Arr: Array[String] = col1.split(",")
            val col2Arr: Array[String] = col2.split(",")
            if (col1Arr.size == 6 && col2Arr.size == 6) {
                val xdata = new Array[Double](6)
                for (i <- 0 until col1Arr.size) {
                    xdata(i) = col1Arr(i).toDouble
                }
                val ydata = new Array[Double](6)
                for (j <- 0 until col2Arr.size) {
                    ydata(j) = col2Arr(j).toDouble
                }
                return getPearsonCorrelationScore(xdata, ydata)
            }
        }
        return 0
    }

    def getPearsonCorrelationScore(xData: Array[Double], yData: Array[Double]): Double = {
        val xMeans = getMeans(xData)
        val yMeans = getMeans(yData)
        var numerator = generateNumerator(xData, xMeans, yData, yMeans)
        var denominator = generateDenomiator(xData, xMeans, yData, yMeans)
        var corr = numerator / denominator
        if(corr.isNaN()){
            corr = 0.0
        }
        var result = corr.formatted("%.6f").toDouble
        println("xMeans="+xMeans)
        println("yMeans="+yMeans)
        println("numerator="+numerator)
        println("denominator="+denominator)
        println("corr="+corr)
        println("result="+result)
        return result
    }

    /**
      * 计算分子
      *
      * @param xData
      * @param xMeans
      * @param yData
      * @param yMeans
      * @return
      */
    def generateNumerator(xData: Array[Double], xMeans: Double, yData: Array[Double], yMeans: Double): Double = {
        var numerator: Double = 0.0
        for (i <- 0 until xData.length) {
            numerator = numerator + (xData(i) - xMeans) * (yData(i) - yMeans)
        }
        return numerator
    }

    /**
      * 计算分母
      *
      * @param xData
      * @param xMeans
      * @param yData
      * @param yMeans
      * @return
      */
    def generateDenomiator(xData: Array[Double], xMeans: Double, yData: Array[Double], yMeans: Double): Double = {
        var xSum: Double = 0.0
        for (i <- 0 until xData.length) {
            xSum = xSum + (xData(i) - xMeans) * (xData(i) - xMeans)
        }
        var ySum: Double = 0.0
        for (i <- 0 until yData.length) {
            ySum = ySum + (yData(i) - yMeans) * (yData(i) - yMeans)
        }
        return Math.sqrt(xSum) * Math.sqrt(ySum)
    }

    /**
      * 计算平均数
      * @param data
      * @return
      */
    def getMeans(data: Array[Double]): Double = {
        var sum: Double = 0.0
        for (i <- 0 until data.length) {
            sum = sum + data(i)
        }
        return sum / data.length
    }
}