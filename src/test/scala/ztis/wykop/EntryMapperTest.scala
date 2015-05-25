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
  
  it should "return error object for error responses" in {
    // given 
    val limitExceededJSON = classpathResource("/limit-exceeded.json")
    val invalidAppKeyJSON = classpathResource("/invalid-app-key.json")
    val followersJSON = classpathResource("/followers.json")
    
    // when
    val limitExceeded = EntryMapper.toError(limitExceededJSON)
    val invalidAppKey = EntryMapper.toError(invalidAppKeyJSON)
    val followers = EntryMapper.toError(followersJSON)
    
    // then
    assert(limitExceeded == Some(WykopAPIError(5, "Limit żądań przekroczony")))
    assert(invalidAppKey == Some(WykopAPIError(1, "Niepoprawny klucz aplikacji")))
    assert(followers == None)
  }
  private def classpathResource(resource: String): String = {
    Source.fromURL(getClass.getResource(resource)).getLines().mkString("\n")  
  } 
}
