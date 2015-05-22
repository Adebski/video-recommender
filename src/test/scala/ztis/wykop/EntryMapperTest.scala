package ztis.wykop

import org.scalatest.FlatSpec

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class EntryMapperTest extends FlatSpec {

  val firstEntry = EntriesFixture.firstVideoEntry

  val secondEntry = EntriesFixture.secondVideoEntry

  "EntryMapper" should "handle single entry" in {
    // given
    val json = classpathResource("/single-entry.json")

    // when
    val entry = EntryMapper.toEntry(json)

    // then
    assert(entry == firstEntry)
  }

  it should "handle multiple entries" in {
    // given
    val json = classpathResource("/multiple-entries.json")

    // when
    val entry = EntryMapper.toEntries(json)

    // then
    assert(entry == Vector(firstEntry, secondEntry))
  }
  
  it should "handle multiple followers" in {
    // given
    val json = classpathResource("/followers.json")
    
    // when
    val followers = EntryMapper.toFollowers(json, ArrayBuffer.empty[String])
    
    // then
    assert(followers.toVector == Vector("login1", "login2", "login3", "login4"))
  }
  
  it should "handle empty followers array" in {
    // given
    val json = classpathResource("/empty-followers.json")

    // when
    val followers = EntryMapper.toFollowers(json, ArrayBuffer.empty[String])

    // then
    assert(followers.toVector == Vector())  
  }
  
  private def classpathResource(resource: String): String = {
    Source.fromURL(getClass.getResource(resource)).getLines().mkString("\n")  
  } 
}
