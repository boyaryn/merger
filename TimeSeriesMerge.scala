// Run like this
//   scala TimeSeriesMerge ~/Time_Series_Folder/

import java.io.{File, FileWriter}

import scala.collection.{Iterator, mutable}
import scala.io.Source

case class Record(it: Iterator[String], date: String, value: Int)

object TimeSeriesMerge {
  def main(args: Array[String]) = {
    if (args.length > 0) {
      val dirName =
        if (args(0).endsWith(File.separator)) args(0)
        else args(0) + File.separator
      val dir = new File(args(0))

      if (dir.exists && dir.isDirectory) {
        // Read the next line from a file and return the result as a Record
        def nextRecord(it: Iterator[String]) = {
          if (it.hasNext) {
            val rowElems = it.next().split(":")
            new Some(Record(it, rowElems(0), rowElems(1).toInt))
          }
          else None
        }

        val fileNames = dir.listFiles.filter(_.isFile).map(_.getPath).toList
        val topRecordsList = fileNames.
          map(x => nextRecord(Source.fromFile(x).getLines())).
          filter(_.isDefined)

        // Records (one from each file being processed) organized in a heap structure
        val pq = mutable.PriorityQueue()(Ordering.by((_: Record).date).reverse)
        for (record <- topRecordsList) record match {
          case Some(r) => pq += r
          case None =>
        }

        val file = new File(dirName + "finalOutput.txt")
        val fw = new FileWriter(file)

        while (pq.nonEmpty) {
          val oldestRecord = pq.dequeue
          var (it, date, value) = oldestRecord match {
            case Record(x, y, z) => (x, y, z)
          }
          val record = nextRecord(it)
          if (record.isDefined) record match {
            case Some(r) => pq += r
            case None =>
          }

          // Take records with the same date as in oldestRecord and sum up corresponding values
          while (pq.nonEmpty && (pq.head.date == date)) {
            val anotherOldestRecord = pq.dequeue
            val record = nextRecord(anotherOldestRecord.it)
            if (record.isDefined) record match {
              case Some(r) => pq += r
              case None =>
            }
            value += anotherOldestRecord.value
          }

          fw.write("%s:%d\r\n".format(date, value))
        }

        fw.close()
      }
    }
  }
}
