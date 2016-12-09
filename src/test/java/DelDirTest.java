import org.junit.Test;

import java.io.File;

/**
 * Created by cwei@cz2r.com on 2016/12/9.
 */
public class DelDirTest {

    @Test
    public void testDelDir() {
        File file = new File("output");
        deleteDir(file);
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}
