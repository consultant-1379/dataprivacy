package org.deidentifier.arx.examples;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SplitHandler {

    Map<String, SplitItem> splitItems = new HashMap<>();


    @JsonIgnoreProperties(ignoreUnknown = true) public static class SplitInfo {
        public List<List<Object>> data;
    }


    public SplitHandler(String fileName, List<String> vipList)  {
        ObjectMapper objectMapper = new ObjectMapper();

        File file = new File(fileName);
        SplitInfo splitInfo = null;
        try {
            splitInfo = objectMapper.readValue(file, SplitInfo.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(splitInfo.data.size());
        for (List<Object> item : splitInfo.data) {
            SplitItem i = new SplitItem(item);
            if (vipList.contains(i.name)) {
                System.out.println("Included: " + i);
                splitItems.put(i.name, i);
            } else {
                System.out.println("DROPPED: " + i);
            }
        }
    }

    public void printIVs(Double min) {
        for (SplitItem i : splitItems.values()) {
            if (i.iv > min) {
                System.out.println(i);
            }
        }
    }

    public void filterIVs(Double min) {
        System.out.println("Old splitMap size: " + splitItems.size());
        Map<String, SplitItem> newMap = new HashMap<>();
        for (SplitItem i : splitItems.values()) {
            if (i.iv > min) {
                newMap.put(i.name, i);
            }
        }
        System.out.println("New splitMap size: " + newMap.size());
        splitItems = newMap;
    }

    public boolean hasFeature(String featureName) {
        return splitItems.get(featureName) != null;
    }

    public Set<String> getFeatures() {
        return splitItems.keySet();
    }

    public String getMapping(String featureName, String value) {
        SplitItem i = splitItems.get(featureName);
        if (i==null) return null;
        String lower = "-inf";
        String result = "UNKNOWN";
        if (!i.categorical) {
            if (value.isEmpty()) {
                return "<EMPTY>";
            }
            Double dv = new Double(value);
            for (Double dd : i.splits) {
                if (dv < dd) {
                    return lower + ".." + dd;
                }
                lower = dd.toString();
            }
            return lower + "-inf";
        } else {
/*            for (List<String > cat : i.categoricalSplits) {
                System.out.println(cat);
                for (String s : cat) {
                    System.out.println(s + " <> " + value);
                    if (s.equals(value)) {
                        System.out.println("juhuu");
                        System.out.println(cat.toString());
                        return cat.toString();
                    }
                }
            }*/
            return "<nocat>";
        }
    }
}
