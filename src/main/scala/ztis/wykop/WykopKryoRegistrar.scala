package ztis.wykop

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar

class WykopKryoRegistrar extends IKryoRegistrar {
  override def apply(kryo: Kryo): Unit = {
    kryo.register(classOf[Entry])
  }
}
