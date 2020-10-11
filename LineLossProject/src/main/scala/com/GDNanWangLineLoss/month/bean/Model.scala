package com.GDNanWangLineLoss.month.bean


/**
  * 城市类     创建City类，输入参数为地市名称及地市代码
  * @param name
  * @param code
  */
case class City(name: String, code: String) {
    override def toString: String = {
        s"Name:$name -> Code:$code"
    }
}