import java.io.FileInputStream
import java.util.Map
import java.util.Properties
import com.bmwcarit.barefoot.road.BaseRoad
import com.bmwcarit.barefoot.road.BfmapWriter
import com.bmwcarit.barefoot.road.PostGISReader
import com.bmwcarit.barefoot.util.SourceException
import com.bmwcarit.barefoot.util.Tuple
import com.bmwcarit.barefoot.roadmap.Loader

object makeMap {

  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties();
    properties.load(new FileInputStream("/home/wt/IdeaProjects/T-Assess/barefoot/config/db.properties"))
    val writer = new BfmapWriter("/home/wt/IdeaProjects/T-Assess/barefoot/bfmap/rome.bfmap")
    val host = properties.getProperty("database.host")
    val port: Int = properties.getProperty("database.port", "0").toInt
    val database = properties.getProperty("database.name")
    val table = properties.getProperty("database.table")
    val user = properties.getProperty("database.user")
    val password = properties.getProperty("database.password")
    val path = properties.getProperty("database.road-types")

    if (host == null || port == 0 || database == null || table == null || user == null
      || password == null || path == null) {
      throw new SourceException("could not read database properties");
    }

    var config: Map[java.lang.Short, Tuple[java.lang.Double, Integer]] = null;
    try {
      config = Loader.read(path);
    } catch {
      case _: Throwable =>
        throw new SourceException("could not read road types from file " + path);
    }

    val gisReader: PostGISReader = new PostGISReader(host, port, database, table, user, password, config)

    writer.open()
    try {
      gisReader.open()
      try {
        var baseroadBuffer: BaseRoad = gisReader.next()
        while (baseroadBuffer != null) {
          writer.write(baseroadBuffer)
          baseroadBuffer = gisReader.next()
        }
      } finally {
        gisReader.close()
      }
    } finally {
      writer.close()
    }
  }
}