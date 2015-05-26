package ztis.wykop

import java.net.URI

object EntriesFixture {
  val firstVideoEntry = Entry("some-author",
    URI.create("https://www.youtube.com/watch?v=PBm8H6NFsGM"), "1")
  val secondVideoEntry = Entry("some-author-2",
    URI.create("https://www.vimeo.com/110554082"), "2")
  val nonVideoEntry = Entry("some-author-3",
    URI.create("https://www.google.com"), "3")
}
