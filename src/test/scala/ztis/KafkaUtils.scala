package ztis

import kafka.admin.TopicCommand

object KafkaUtils {
  def createTopic(zookeeper: String, topicName: String): Unit = {
    TopicCommand.main(Array("--zookeeper",
      zookeeper, "--create", "--topic", topicName, "--partitions", "1", "--replication-factor", "1"))
  }
  
  def removeTopic(zookeeper: String, topicName: String): Unit = {
    TopicCommand.main(Array("--zookeeper",
      zookeeper, "--delete", "--topic", topicName))
  }
}
