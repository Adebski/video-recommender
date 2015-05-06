package ztis.twitter

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar

class TweetKryoRegistrar extends IKryoRegistrar {
  override def apply(kryo: Kryo): Unit = {
    kryo.register(classOf[Tweet])
  }
}
