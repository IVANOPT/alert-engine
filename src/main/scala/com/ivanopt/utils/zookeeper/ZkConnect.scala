package com.ivanopt.utils.zookeeper

import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

/**
  * Created by Ivan on 08/06/2018.
  */
class ZkConnect {

  private var zk: ZooKeeper = _
  private val connSignal: CountDownLatch = new CountDownLatch(0)

  def connect(host: String): ZooKeeper = {
    if (zk != null) {
      return zk
    }

    zk = new ZooKeeper(host, 60000, new Watcher {
      override def process(watchedEvent: WatchedEvent): Unit = {
        if (watchedEvent.getState == KeeperState.SyncConnected) {
          connSignal.countDown()
        }
      }
    })
    connSignal.await()
    return zk
  }

  def close(): Unit = {
    zk.close()
  }

  def createNode(path: String, data: Array[Byte]): Unit = {
    zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  def updateNode(path: String, data: Array[Byte]): Unit = {
    zk.setData(path, data, zk.exists(path, true).getVersion)
  }

  def deleteNode(path: String): Unit = {
    zk.delete(path, zk.exists(path, true).getVersion)
  }

}
