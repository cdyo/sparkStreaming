package NingXia;
import java.io.*;
import java.util.*;

public class PropUtil {
    public static Properties getpro(String args) throws IOException {
        Properties pro = new Properties();
        File file = new File(args);
        if (!file.exists())
            System.out.println("conf file non-existent");
        FileInputStream fis = new FileInputStream(file);
        try {
            pro.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return pro;
    }

    public static Map<String, Map<String, Integer>> getvalue(String args) throws IOException {
        Properties pro = getpro(args);
        Set set = pro.keySet();
        Iterator it = set.iterator();
        int size = 0;
        while (it.hasNext()) {
            if (it.next().toString().contains("flag")) {
                size = size + 1;
            } else
                continue;
        }

        HashMap hashmap = new HashMap<String, Map<String, Integer>>();
        for (int i = 1; i <= size; i++) {
            Map tmap = new HashMap<String, Integer>();
            tmap.put(pro.getProperty("topic" + i), new Integer(pro.getProperty("index" + i)));
            hashmap.put(pro.getProperty("flag" + i), tmap);
        }
        return hashmap;
    }

    public static Set<String> gettopics(String args) throws IOException {
        Set set = new HashSet<String>();
        HashMap hashmap = (HashMap) getvalue(args);
        Collection<Map> c = hashmap.values();
        for (Map map : c) {
            set.addAll(map.keySet());
        }
        return set;
    }
}
