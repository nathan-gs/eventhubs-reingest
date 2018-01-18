package gs.nathan.eventhubsreingest.input

case class InputSpec(alias: String, path: String, format: String, options: Map[String, String])
object InputSpec {
  def apply(alias: String, kv:Map[String, String]): InputSpec = {
    val path = kv("path")
    val format = kv("format")
    val options = kv.filterKeys(_.startsWith("options")).map(ov => {
      val key = ov._1.stripPrefix("options.")
      (key, ov._2)
    })

    InputSpec(alias, path, format, options)
  }
}