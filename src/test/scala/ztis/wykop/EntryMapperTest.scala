package ztis.wykop

import org.scalatest.FlatSpec

import scala.io.Source

class EntryMapperTest extends FlatSpec {

  val firstEntry = EntriesFixture.firstVideoEntry

  val secondEntry = EntriesFixture.secondVideoEntry

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
