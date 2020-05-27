import com.atguigu.gmall.common.util.PropertyUtil

/**
 * Author atguigu
 * Date 2020/5/27 16:37
 */
object UtilTest {
    def main(args: Array[String]): Unit = {
//        val r: String = PropertyUtil.getProperty("config.properties", "kafka.servers")
        val r: String = PropertyUtil.getProperty("config.properties", "kafka.group.id")
        println(r)
    }
}
