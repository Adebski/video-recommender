package ztis.wykop

import org.scalatest.FlatSpec

import scala.io.Source

class EntryMapperTest extends FlatSpec {
  
  val firstEntry = Entry("some-author", 
    "https://www.youtube.com/watch?v=PBm8H6NFsGM")
  val secondEntry = Entry("some-author-2", 
    "https://www.youtube.com/watch?v=QGTbGoLEom8")
  
  "EntryMapper" should "handle single value" in {
    // given
    val json = Source.fromURL(getClass.getResource("/single-entry.json")).getLines().mkString("\n")
    
    // when
    val entry = EntryMapper.toEntry(json)
    
    // then
    assert(entry == firstEntry)
  }
  
  it should "handle multiple values" in {
    // given
    val json = Source.fromURL(getClass.getResource("/multiple-entries.json")).getLines().mkString("\n")

    // when
    val entry = EntryMapper.toEntries(json)

    // then
    assert(entry == Vector(firstEntry, secondEntry)) 
  }
}
