package com.ivanopt.utils.zookeeper

import java.util
import java.util.Date

import org.apache.commons.lang3.StringUtils
import org.apache.zookeeper.ZooKeeper
import org.testng.annotations.Test

import scala.collection.JavaConversions._

class ZkConnectTest {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[ZkConnectTest])

  private final val TEST_NODE: String = "/kafka_test_node"

  @Test
  def createNode(): Unit = {
    val connector: ZkConnect = new ZkConnect()
    val zk: ZooKeeper = connector.connect("localhost:1102")
    val currentData: Date = new Date()
    val testCreateNode: String = TEST_NODE + "_" + currentData.toString()
    connector.createNode(testCreateNode, currentData.toString().getBytes())
    val zNodes: util.List[String] = zk.getChildren("/", true)
    zNodes.toList.foreach { node => println(node) }
    var data: Array[Byte] = zk.getData(testCreateNode, true, zk.exists(testCreateNode, true))
    log.info("GetData once creating node...")
    val savedData: String = (data.map(_.toChar)).mkString
    log.info("Saved data: ".concat(savedData))
    assert(StringUtils.equals(savedData, currentData.toString))
  }

  @Test
  def updateNode(): Unit = {
    val connector: ZkConnect = new ZkConnect()
    val zk: ZooKeeper = connector.connect("localhost:1102")
    val currentData: Date = new Date()
    val testUpdateNode: String = TEST_NODE
    if (zk.exists(TEST_NODE, true) == null) {
      connector.createNode(testUpdateNode, currentData.toString().getBytes())
    }
    connector.updateNode(testUpdateNode, currentData.toString.getBytes())

    var data: Array[Byte] = zk.getData(testUpdateNode, true, zk.exists(testUpdateNode, true))
    log.info("GetData once updating node...")

    val updatedData: String = (data.map(_.toChar)).mkString
    log.info("Saved data: ".concat(updatedData))

    assert(StringUtils.equals(updatedData, currentData.toString))
  }

  def main(args: Array[String]): Unit = {

    val connector: ZkConnect = new ZkConnect()
    val zk: ZooKeeper = connector.connect("localhost:1102")
    val newNode: String = "/kafka" + new Date()
    connector.createNode(newNode, new Date().toString().getBytes())
    val zNodes: util.List[String] = zk.getChildren("/", true)
    zNodes.toList.foreach { node => println(node) }

    var data: Array[Byte] = zk.getData(newNode, true, zk.exists(newNode, true))
    println("GetData before setting")
    println((data.map(_.toChar)).mkString)

    println("GetData after setting")
    connector.updateNode(newNode, "Modified data".getBytes())
    data = zk.getData(newNode, true, zk.exists(newNode, true))
    println((data.map(_.toChar)).mkString)

  }

}
