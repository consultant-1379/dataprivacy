package org.deidentifier.arx.examples;

import java.util.ArrayList;
import java.util.List;

public class SplitItem {
    String name;
    Double iv;
    Boolean categorical;
    List<Double> splits;
    List<List<String>> categoricalSplits;


    @Override
    public String toString() {
        return "SplitItem{" +
                "name='" + name + '\'' +
                ", iv=" + iv +
                ", categorical=" + categorical +
                ", splits=" + splits +
                ", categoricalSplits=" + categoricalSplits +
                '}';
    }

    SplitItem(List<Object> list) {
        name = list.get(0).toString();
        iv = (Double) list.get(3);
        //categorical = "Yes".equals(list.get(4));
        categorical = false;
        if (categorical) {
            System.out.println(list.get(1));
            List<Object> categories = (List<Object>) list.get(1);
            categoricalSplits = new ArrayList<>();
            for (Object categoryObject:categories) {
                List<Object> category = (List<Object>) categoryObject;
                List<String> result = new ArrayList<>();
                System.out.println(category);
                for (Object ciObject:category) {
                    result.add(ciObject.toString());
                }
                categoricalSplits.add(result);
            }
        } else {
            splits = new ArrayList<Double>((List<Double>) list.get(1));
        }
    }

}
