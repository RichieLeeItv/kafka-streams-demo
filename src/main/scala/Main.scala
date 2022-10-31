import com.sksamuel.avro4s.BinaryFormat
import com.sksamuel.avro4s.kafka.GenericSerde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{
  KeyValueStore,
  QueryableStoreTypes,
  ReadOnlyKeyValueStore
}
import org.apache.kafka.streams.{
  KafkaStreams,
  StoreQueryParameters,
  StreamsConfig
}

import java.util.Properties

case class License(showId: Int, exp: Long)
case class ContentDisplayWindow(start: Long, end: Long)
case class Content(license: License, displayWindow: ContentDisplayWindow) {
  // we would also wantto check that the current time is within this but you get the idea...
  def isWatchable: Boolean =
    license.exp > displayWindow.start && license.exp < displayWindow.end
}

object Main extends App {
  val licenseTopic = "licenses"
  val cdwTopic = "contentDisplay"

  val builder = new StreamsBuilder()

  // thanks to org.apache.kafka.streams.scala.ImplicitConversions._
  // we no longer need to explicitly defined our Consumed[K,V] instance!
  implicit val licenceSerde =
    new com.sksamuel.avro4s.kafka.GenericSerde[License](BinaryFormat)
  implicit val cdwSerde = new GenericSerde[ContentDisplayWindow](BinaryFormat)
  implicit val contentSerde = new GenericSerde[Content](BinaryFormat)
  implicit val intSerde = Serdes.intSerde

  // setting up a kafka consumer and producer using values in the KV and producer of the KV inside the KeyValueStore params.
  val licenseStoreMaterialization
      : Materialized[Int, License, KeyValueStore[Bytes, Array[Byte]]] =
    Materialized
      .as[Int, License, KeyValueStore[Bytes, Array[Byte]]](
        "licenseStore"
      ) // store name, not exactly the top
      .withKeySerde(intSerde)
      .withValueSerde(licenceSerde)

  // global K-Table: Creates a state store across all parititons to query against.
  // this is backed by a topic which is defined in the licenseStoreMaterialization
  val licenseTable: GlobalKTable[Int, License] =
    builder.globalTable(licenseTopic, licenseStoreMaterialization)
  val contentDisplayStream: KStream[Int, ContentDisplayWindow] =
    builder.stream(cdwTopic)

  contentDisplayStream
    .join[Int, License, Content](licenseTable)(
      (k, _) => k, // select the key to be used for the join.
      (cdw, license) => Content(license, cdw)
    ) // join
    .filter((_, content) => content.isWatchable)
    .to("watchableContent")

  val props = new Properties
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val streams = new KafkaStreams(builder.build(), props)

  streams.start();
}

class contentDisplayStore(streams: KafkaStreams) {
  val qp = StoreQueryParameters.fromNameAndType(
    "watchableContent",
    QueryableStoreTypes.KeyValueStoreType[Int, Content]
  )
  val thing: ReadOnlyKeyValueStore[Int, Content] = streams.store(qp)

  val show123Content = Option(
    thing.get(123)
  ) // handles for nulls from Kafka Streams
}
