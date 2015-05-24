package ztis.relationships

case class FollowersBuilder[T](page: Long, gatheredFollowers: Vector[T] = Vector.empty[T], partialResult: Boolean = true)
