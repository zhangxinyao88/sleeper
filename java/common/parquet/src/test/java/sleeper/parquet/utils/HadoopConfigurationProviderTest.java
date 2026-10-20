/*
 * Copyright 2022-2026 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.instance.CommonProperty.S3_UPLOAD_BLOCK_SIZE;
import static sleeper.core.properties.table.TableProperty.S3A_READAHEAD_RANGE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

class HadoopConfigurationProviderTest {
    @TestFactory
    Stream<DynamicTest> shouldApplyConfiguredReadaheadForIngestAndCompaction() {
        return Stream.of(Map.entry("0", 0L), Map.entry("8192", 8192L), Map.entry("32M", 33554432L))
                .map(entry -> dynamicTest(entry.getKey(), () -> {
                    InstanceProperties properties = propertiesWithReadahead(entry.getKey());

                    Configuration configuration = HadoopConfigurationProvider.getConfigurationForECS(properties);

                    assertThat(configuration.getLongBytes("fs.s3a.readahead.range", -1)).isEqualTo(entry.getValue());
                    assertThat(configuration.get("fs.s3a.block.size")).isEqualTo(properties.get(S3_UPLOAD_BLOCK_SIZE));
                }));
    }

    @Test
    void shouldKeepHadoopDefaultWhenReadaheadIsUnset() {
        Configuration configuration = HadoopConfigurationProvider.getConfigurationForECS(createTestInstanceProperties());

        assertThat(configuration.get("fs.s3a.readahead.range"))
                .isEqualTo(new Configuration().get("fs.s3a.readahead.range"));
    }

    @Test
    void shouldKeepHadoopDefaultWhenReadaheadIsEmpty() {
        Configuration configuration = HadoopConfigurationProvider.getConfigurationForECS(propertiesWithReadahead(""));

        assertThat(configuration.get("fs.s3a.readahead.range"))
                .isEqualTo(new Configuration().get("fs.s3a.readahead.range"));
    }

    @Test
    void shouldKeepTableQueryReadaheadIndependentOfTaskReadahead() {
        InstanceProperties properties = propertiesWithReadahead("8M");
        TableProperties tableProperties = new TableProperties(properties);
        tableProperties.set(S3A_READAHEAD_RANGE, "2M");

        Configuration taskConfiguration = HadoopConfigurationProvider.getConfigurationForECS(properties);
        Configuration queryConfiguration = HadoopConfigurationProvider.getConfigurationForQueryLambdas(properties, tableProperties);

        assertThat(taskConfiguration.getLongBytes("fs.s3a.readahead.range", -1)).isEqualTo(8 * 1024 * 1024);
        assertThat(queryConfiguration.getLongBytes("fs.s3a.readahead.range", -1)).isEqualTo(2 * 1024 * 1024);
    }

    @TestFactory
    Stream<DynamicTest> shouldRejectInvalidReadaheadConfiguration() {
        return Stream.of("-1", "not-bytes", "1.5M", "1MB")
                .map(value -> dynamicTest(value, () -> assertThat(propertiesWithReadahead(value).isValid()).isFalse()));
    }

    @TestFactory
    Stream<DynamicTest> shouldAcceptValidReadaheadConfiguration() {
        return Stream.of("0", "8192", "32M", "")
                .map(value -> dynamicTest("Value: " + value, () -> assertThat(propertiesWithReadahead(value).isValid()).isTrue()));
    }

    private InstanceProperties propertiesWithReadahead(String value) {
        return InstanceProperties.createWithoutValidation(loadProperties(
                createTestInstanceProperties().saveAsString() + "\nsleeper.fs.s3a.readahead.range=" + value));
    }
}
