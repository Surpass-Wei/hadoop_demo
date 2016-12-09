import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Created by cwei@cz2r.com on 2016/12/9.
 */
public class APITest {

    @Test
    public void TestConfiguration() {
        Configuration conf = new Configuration();
        conf.addResource("D:\\Other_Projects\\hadoopobject\\src\\main\\resources\\hadoop\\core-site.xml");
        System.out.println(conf.get("hadoop.tmp.dir"));
        System.out.println(conf.get("io.file.buffer.size"));

    }
}
