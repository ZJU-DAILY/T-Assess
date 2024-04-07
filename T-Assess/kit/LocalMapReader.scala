package kit

import java.lang.{Short => JShort}
import java.util.{HashSet => JHashSet}
import java.io.ObjectInput
import java.io.ObjectInputStream
import java.io.FileNotFoundException
import java.io.IOException
import com.esri.core.geometry.GeometryEngine
import com.esri.core.geometry.Polygon
import com.esri.core.geometry.SpatialReference
import com.bmwcarit.barefoot.road.BaseRoad
import com.bmwcarit.barefoot.road.RoadReader
import com.bmwcarit.barefoot.util.SourceException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class LocalMapReader(uri: String) extends RoadReader {
  private var reader : Option[ObjectInput] = None
  private var exclusions : JHashSet[JShort] = _
  private var polygon : Polygon = _
  override def isOpen: Boolean = reader.nonEmpty
  override def open(): Unit = {
    open(null, null)
  }

  override def open(polygon : Polygon, exclusions : JHashSet[JShort]) : Unit = {
    try {
      val conf = new Configuration()
      val fs = FileSystem.getLocal(conf)
      val in = fs.open(new Path(uri))
      this.reader = Some(new ObjectInputStream(in))
      this.exclusions = exclusions
      this.polygon = polygon
    } catch {
      case _: FileNotFoundException =>
        throw new SourceException("File could not be found for uri: " + uri)
      case _: IOException =>
        throw new SourceException("Opening reader failed")
    }
  }

  override def close(): Unit = {
    try {
      reader foreach { _.close }
      reader = None
    } catch {
      case _: IOException =>
        throw new SourceException("Closing file failed.")
    }
  }

  override def next() : BaseRoad = {
    if (!isOpen()) {
      throw new SourceException("File is closed or invalid.")
    }
    reader match {
      case Some(r) =>
        try {
          var road : BaseRoad = null
          do {
            road = r.readObject().asInstanceOf[BaseRoad]
            if (road == null) {
              return null
            }
          } while (
            exclusions != null && exclusions.contains(road.`type`())
              ||
              polygon != null
                && !GeometryEngine.contains(polygon, road.geometry(), SpatialReference.create(4326))
                && !GeometryEngine.overlaps(polygon, road.geometry(), SpatialReference.create(4326))
          )
          road
        } catch {
          case _: ClassNotFoundException =>
            throw new SourceException("File is corrupted, read object is not a road.")
          case _: IOException =>
            throw new SourceException("Reading file failed")
        }
      case None =>
        throw new SourceException("File not open")
    }
  }
}