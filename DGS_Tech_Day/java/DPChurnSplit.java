/*
 * ARX: Powerful Data Anonymization
 * Copyright 2012 - 2020 Fabian Prasser and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.deidentifier.arx.examples;

import org.deidentifier.arx.*;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.AttributeType.Hierarchy.DefaultHierarchy;
import org.deidentifier.arx.DataGeneralizationScheme.GeneralizationDegree;
import org.deidentifier.arx.criteria.EDDifferentialPrivacy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * This class implements an example of how to use (e,d)-DP
 *
 * @author Fabian Prasser
 * @author Florian Kohlmayer
 */
public class DPChurnSplit extends Example {


    //final static List<String> vipList = Arrays.asList("og_others_", "roam_og_mou_", "std_ic_t2m_mou_", "loc_og_mou_", "std_og_t2f_mou_", "std_ic_t2f_mou_", "loc_og_t2m_mou_", "loc_og_t2c_mou_", "std_og_t2m_mou_", "loc_ic_t2t_mou_");
    final static List<String> vipList = Arrays.asList("og_others_6", "roam_og_mou_8", "std_ic_t2m_mou_6", "loc_og_mou_6", "std_og_t2f_mou_6", "std_ic_t2f_mou_8", "loc_og_t2m_mou_6", "loc_og_t2c_mou_8", "std_og_t2m_mou_7", "loc_ic_t2t_mou_7");

    static SplitHandler splithandler = new SplitHandler("split2.json", vipList);

    /**
     * Entry point.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws IOException {

        splithandler.filterIVs(0d);

        // Define data

        //Data data = Data.create("data/telecom_churn_data.csv", StandardCharsets.UTF_8, ',');
        //Data data = Data.create("data/churn5crop.csv", StandardCharsets.UTF_8, ',');

        //DataSource source = DataSource.createCSVSource("data/telecom_churn_data.csv", StandardCharsets.UTF_8, ',', true);
        //DataSource source = DataSource.createCSVSource("data/toni_dp.csv", StandardCharsets.UTF_8, ',', true);
        DataSource source = DataSource.createCSVSource("data/Toni_Attila_SMOTEos.csv", StandardCharsets.UTF_8, ',', true);

        /*for (String feature: splithandler.getFeatures()) {
            source.addColumn(feature);
        }*/
        source.addColumn("count");
        for (String feature: vipList) {
            source.addColumn(feature);
            //source.addColumn(feature + "7");
            //source.addColumn(feature + "8");
        }
        source.addColumn("churn");
        Data data = Data.create(source);

        setupColumns(data);

        // Create an instance of the anonymizer
        ARXAnonymizer anonymizer = new ARXAnonymizer();

        // Create a differential privacy criterion
        //EDDifferentialPrivacy criterion = new EDDifferentialPrivacy(3d, 0.01d, DataGeneralizationScheme.create(data, GeneralizationDegree.MEDIUM_HIGH));

        EDDifferentialPrivacy criterion = new EDDifferentialPrivacy(1d, 0.00001d,
                DataGeneralizationScheme.create(data, GeneralizationDegree.MEDIUM_HIGH));
                //null);

        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(criterion);
        config.setSuppressionLimit(1d);
        config.setDPSearchBudget(1d);
        config.setHeuristicSearchStepLimit(300);
        ARXResult result = null;
        result = anonymizer.anonymize(data, config);

        // Access output
        DataHandle optimal = result.getOutput();

        System.out.println(result.isResultAvailable());

        // Print input
        System.out.println(" - Input data:");
        //printHandle(data.getHandle());

        System.out.println(" - Result:");
        //printHandle(optimal);
        optimal.save("lofasz.csv");
        System.out.println(result.getProcessStatistics().getNumberOfSteps());
    }

    private static void setupColumns(Data data) {
        for (int i = 0; i < data.getHandle().getNumColumns(); i++) {
            setup(data, i);
        }
    }

    private static void setup(Data data, int i) {
        String name = data.getHandle().getAttributeName(i);
        //if (splithandler.hasFeature(name)) {
        if (isVip(name)) {
            data.getDefinition().setAttributeType(name, AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
            DefaultHierarchy h = Hierarchy.create();
            String[] values = data.getHandle().getDistinctValues(i);
            for (String value : values) {
                String mappedValue = splithandler.getMapping(name, value);
                h.add(value, mappedValue, "@");
            }
            data.getDefinition().setHierarchy(name, h);
        } else if (name.equals("churn")) {
            data.getDefinition().setAttributeType(name, AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
            DefaultHierarchy h = Hierarchy.create();
            h.add("0", "0", "CH");
            h.add("1", "1", "CH");
            data.getDefinition().setHierarchy(name, h);
        } else {
            data.getDefinition().setAttributeType(name, AttributeType.INSENSITIVE_ATTRIBUTE);
/*
            String[] values = data.getHandle().getDistinctValues(i);
            for (String value : values) {
                data.getHandle().replace(i, value, "--");
            }
*/
        }
    }

    private static boolean isVip(String featureName) {
        for (String s:vipList) {
            if (featureName.startsWith(s)) {
                System.out.println("VIP: " + featureName);
                return true;
            }
        }
        return false;
    }

}
