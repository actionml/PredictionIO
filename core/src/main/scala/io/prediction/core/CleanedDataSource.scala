package io.prediction.core

import grizzled.slf4j.Logger
import io.prediction.annotation.DeveloperApi
import io.prediction.data.storage.{DataMap, Event}
import io.prediction.data.store.{Common, LEventStore, PEventStore}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.concurrent.duration.Duration

/** :: DeveloperApi ::
  * Base class of cleaned data source.
  *
  * A cleaned data source consists tools for cleaning events that happened earlier that
  * specified duration in seconds from train moment. Also it can remove duplicates and compress
  * properties(flat set/unset events to one)
  *
  */
@DeveloperApi
trait CleanedDataSource {

  /** :: DeveloperApi ::
    * Current App name which events will be cleaned.
    *
    * @return App name
    */
  @DeveloperApi
  def appName: String

  /** :: DeveloperApi ::
    * Param list that used for cleanup.
    *
    * @return current event windows that will be used to clean up events.
    */
  @DeveloperApi
  def eventWindow: Option[EventWindow] = None

  @transient lazy val logger = Logger[this.type]

  /** :: DeveloperApi ::
    *
    * Returns RDD of events happend after duration in event window params.
    *
    * @return RDD[Event] most recent PEvents.
    */
  @DeveloperApi
  def getCleanedPEvents(sc: SparkContext): RDD[Event] = {
    val pEvents = PEventStore.find(appName)(sc)

    eventWindow
      .flatMap(_.duration)
      .map { duration =>
        val fd = Duration(duration)
        pEvents.filter(e =>
          e.eventTime.isAfter(DateTime.now().minus(fd.toMillis))
        )
      }.getOrElse(pEvents)
  }

  /** :: DeveloperApi ::
    *
    * Returns Iterator of events happend after duration in event window params.
    *
    * @return Iterator[Event] most recent LEvents.
    */
  @DeveloperApi
  def getCleanedLEvents(): Iterable[Event] = {

    val lEvents = LEventStore.find(appName)

    eventWindow
      .flatMap(_.duration)
      .map { duration =>
        val fd = Duration(duration)
        lEvents.filter(e =>
          e.eventTime.isAfter(DateTime.now().minus(fd.toMillis))
        )
      }.getOrElse(lEvents).toIterable
  }

  def compressPProperties(sc: SparkContext, rdd: RDD[Event]): RDD[Event] = {
    rdd.filter(isSetEvent)
      .groupBy(_.entityType)
      .map { pair =>
        val (_, ls) = pair
        compress(ls)
      } ++ rdd.filter(!isSetEvent(_))
  }

  def compressLProperties(events: Iterable[Event]): Iterable[Event] = {
    events.filter(isSetEvent).toIterable
      .groupBy(_.entityType)
      .map { pair =>
        val (_, ls) = pair
        compress(ls)
      } ++ events.filter(!isSetEvent(_))
  }

  def removePDuplicates(sc: SparkContext, rdd: RDD[Event]): RDD[Event] = {
    rdd.distinct()
  }

  def removeLDuplicates(ls: Iterable[Event]): Iterable[Event] = {
    ls.toList.distinct
  }

  /** :: DeveloperApi ::
    *
    * Filters most recent, compress properties and removes duplicates of PEvents
    *
    * @return RDD[Event] most recent PEvents
    */
  @DeveloperApi
  def cleanedPEvents(sc: SparkContext): RDD[Event] = {
    val stage1 = getCleanedPEvents(sc)
    val result = cleanPEvents(sc, stage1)
    val (appId, channelId) = Common.appNameToId(appName, None)
    PEventStore.wipe(result, appId, channelId)(sc)
    result
  }

  /** :: DeveloperApi ::
    *
    * Filters most recent, compress properties of PEvents
    */
  @DeveloperApi
  def cleanPEvents(sc: SparkContext, rdd: RDD[Event]): RDD[Event] = {
    eventWindow match {
      case Some(ew) =>
        var updated =
          if (ew.compressProperties) compressPProperties(sc, rdd) else rdd
        //if (ew.removeDuplicates) removePDuplicates(updated)
        updated
      case None =>
        rdd
    }
  }

  /** :: DeveloperApi ::
    *
    * Filters most recent, compress properties and removes duplicates of LEvents
    *
    * @return Iterator[Event] most recent LEvents
    */
  @DeveloperApi
  def cleanedLEvents: Iterable[Event] = {
    val stage1 = getCleanedLEvents()
    val result = cleanLEvents(stage1)
    val (appId, channelId) = Common.appNameToId(appName, None)
    LEventStore.wipe(result, appId, channelId)
    result
  }

  /** :: DeveloperApi ::
    *
    * Filters most recent, compress properties of LEvents
    */
  @DeveloperApi
  def cleanLEvents(ls: Iterable[Event]): Iterable[Event] = {
    eventWindow match {
      case Some(ew) =>
        var updated =
          if (ew.compressProperties) compressLProperties(ls) else ls
        //if (ew.removeDuplicates) removePDuplicates(updated)
        updated
      case None =>
        ls
    }
  }

  private def isSetEvent(e: Event): Boolean = {
    e.event == "set" || e.event == "unset"
  }

  private def compress(events: Iterable[Event]): Event = {
    events.find(_.event == "set") match {

      case Some(first) =>
        events.reduce { (e1, e2) =>
          val props = e2.event match {
            case "set" =>
              e1.properties.fields ++ e2.properties.fields
            case "unset" =>
              e1.properties.fields
                .filterKeys(f => !e2.properties.fields.contains(f))
          }
          e1.copy(properties = DataMap(props))
        }

      case None =>
        events.reduce { (e1, e2) =>
          e1.copy(properties =
            DataMap(e1.properties.fields ++ e2.properties.fields)
          )
        }
    }
  }
}

case class EventWindow(
  duration: Option[String] = None,
  removeDuplicates: Boolean = false,
  compressProperties: Boolean = false
)
