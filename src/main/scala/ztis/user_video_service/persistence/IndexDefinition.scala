package ztis.user_video_service.persistence

import org.neo4j.graphdb.{DynamicLabel, Label}

case class IndexDefinition(label: Label, property: String)

object IndexDefinition {
  def apply(label: String, property: String): IndexDefinition = {
    IndexDefinition(DynamicLabel.label(label), property)
  }
}
