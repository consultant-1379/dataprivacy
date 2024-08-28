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
import org.deidentifier.arx.DataType;
import org.deidentifier.arx.DataType.DataTypeDescription;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * This class implements an example of how to use (e,d)-DP
 *
 * @author Fabian Prasser
 * @author Florian Kohlmayer
 */
public class DPChurn extends Example {

    /**
     * Entry point.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws IOException {

        // Define data

        Data data = Data.create("data/telecom_churn_data.csv", StandardCharsets.UTF_8, ',');
        //Data data = Data.create("data/churn5crop.csv", StandardCharsets.UTF_8, ',');


        printColumns(data);

        // Create an instance of the anonymizer
        ARXAnonymizer anonymizer = new ARXAnonymizer();

        // Create a differential privacy criterion
        //EDDifferentialPrivacy criterion = new EDDifferentialPrivacy(3d, 0.01d, DataGeneralizationScheme.create(data, GeneralizationDegree.MEDIUM_HIGH));

        EDDifferentialPrivacy criterion = new EDDifferentialPrivacy(2.5d, 0.01d,
                DataGeneralizationScheme.create(data, GeneralizationDegree.MEDIUM));

        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(criterion);
        config.setSuppressionLimit(1d);
        config.setDPSearchBudget(0.2d);
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

    private static void printColumns(Data data) {
        for (int i = 0; i < data.getHandle().getNumColumns(); i++) {
            setup(data, i);
        }
    }

    private static void setup(Data data, int i) {
        String name = data.getHandle().getAttributeName(i);
        data.getDefinition().setAttributeType(name, AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        String[] values = data.getHandle().getDistinctValues(i);

        DataType type = null;
        if (i == 0) {
            data.getDefinition().setAttributeType(name, AttributeType.INSENSITIVE_ATTRIBUTE);
            type = DataType.STRING;
            //data.getDefinition().setHierarchy(name, getHierarchyForMobileNumber(values));
            System.out.println(i);
        } else if (values.length < 4) {
            type = DataType.STRING;
            data.getDefinition().setHierarchy(name, getHierarchyForEnum(values));
        } else if (Arrays.stream(values).anyMatch(s -> s.contains("/"))) {
            type = DataType.createDate("MM/dd/yyyy");
            data.getDefinition().setHierarchy(name, getHierarchyForEnum(values));
        } else if (Arrays.stream(values).anyMatch(s -> s.contains("."))) {
            type = DataType.DECIMAL;
            data.getDefinition().setHierarchy(name, getHierarchyForDecimalAlex(values));
        } else {
            type = DataType.INTEGER;
            data.getDefinition().setHierarchy(name, getHierarchyForDecimalAlex(values));
        }

        data.getDefinition().setDataType(name, type);
        System.out.println(name);
        System.out.println(type);
    }

    static Hierarchy getHierarchyForDecimal(String[] values) {
        DefaultHierarchy h = Hierarchy.create();
        for (String value : values) {
            if (value.isEmpty()) {
                h.add(value, "<EMPTY>", "*");
                continue;
            }
            Double d = new Double(value);
            if (d < 0) {
                h.add(value, "neg", "*");
            } else if (d == 0) {
                h.add(value, "zero", "*");
            } else if (true) {
                Integer i = (int) (Math.log(d) * 10);
                h.add(value, i.toString(), "*");
            } else {
                Long i = Math.round(d / 20) * 20;
                h.add(value, i.toString(), "*");
            }
        }
        System.out.println(h.getHierarchy().length);
        System.out.println();
        return h;
    }

    static Hierarchy getHierarchyForDecimalAlex(String[] values) {
        DefaultHierarchy h = Hierarchy.create();
        for (String value : values) {
            if (value.isEmpty()) {
                h.add(value, "<EMPTY>", "D");
                continue;
            }
            Double d = new Double(value);
            if (d < 200) {
                h.add(value, "<200", "D");
            } else {
                h.add(value, ">=200", "D");
            }
        }
        System.out.println(h.getHierarchy().length);
        System.out.println();
        return h;
    }

    static Hierarchy getHierarchyForinteger(String[] values) {
        DefaultHierarchy h = Hierarchy.create();
        for (String value : values) {
            if (value.isEmpty()) {
                h.add(value, "<EMPTY>", "I");
                continue;
            }
            Integer d = new Integer(value);
            h.add(value, ((Integer) (d / 5000)).toString(), "I");
        }
        return h;
    }

    static Hierarchy getHierarchyForEnum(String[] values) {
        DefaultHierarchy h = Hierarchy.create();
        int i = 2;
        for (String value : values) {
            if (i > 0) {
                h.add(value, value, "X");
                i--;
            } else {
                h.add(value, "X2", "X");
            }
        }
        return h;
    }

    static Hierarchy getHierarchyForMobileNumber(String[] values) {
        DefaultHierarchy h = Hierarchy.create();
        for (String value : values) {
            //System.out.println(value.substring(8,9));
            h.add(value, value.substring(8, 10), "PHONE_NUM");
        }
        return h;
    }

}
