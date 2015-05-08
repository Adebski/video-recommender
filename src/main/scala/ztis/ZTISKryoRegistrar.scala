package ztis

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar
import org.apache.spark.serializer.KryoRegistrator
import ztis.twitter.Tweet
import ztis.wykop.Entry

class ZTISKryoRegistrar extends IKryoRegistrar with KryoRegistrator {
  override def apply(kryo: Kryo): Unit = {
    this.registerClasses(kryo)
  }

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Tweet])
    kryo.register(classOf[Entry])  
  }
}
