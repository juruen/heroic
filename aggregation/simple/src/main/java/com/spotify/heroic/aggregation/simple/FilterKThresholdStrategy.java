/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import lombok.Data;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FilterKThresholdStrategy implements FilterStrategy {
    public enum FilterType {
        ABOVE,
        BELOW
    }

    private final FilterType filterType;
    private final double k;

    public FilterKThresholdStrategy(FilterType filterType, double k) {
        this.filterType = filterType;
        this.k = k;
    }

    @Override
    public <T> List<T> filter(List<FilterableMetrics<T>> metrics) {
        return metrics.stream()
            .map(this::buildMaxMin)
            .filter(m -> m.getValue() != null)
            .filter(buildThresholdFilter())
            .map(Extreme::getFilterableMetrics)
            .map(FilterableMetrics::getData)
            .collect(Collectors.toList());
    }

    private <T> Predicate<Extreme<T>> buildThresholdFilter() {
        if (filterType == FilterType.ABOVE) {
            return (a) -> a.getValue() > k;
        } else {
            return (a) -> a.getValue() < k;
        }
    }

    private <T> Extreme<T> buildMaxMin(FilterableMetrics<T> filterableMetrics) {
        if (filterType == FilterType.ABOVE) {
            return new Extreme<>(filterableMetrics, Extreme.What.MAX);
        } else {
            return new Extreme<>(filterableMetrics, Extreme.What.MIN);
        }
    }

    public double getK() {
        return k;
    }

    @Data
    private static class Extreme<T> {
        public enum What {
            MAX,
            MIN
        }

        private final FilterableMetrics<T> filterableMetrics;
        private final Double value;

        public Extreme(FilterableMetrics<T> filterableMetrics, What what) {
            this.filterableMetrics = filterableMetrics;
            this.value = compute(what, filterableMetrics.getMetricSupplier().get());
        }

        private Double compute(What what, MetricCollection metrics) {
            final Stream<Double> stream = metrics.getDataAs(Point.class)
                .stream()
                .map(Point::getValue);

            if (what == What.MAX) {
                return stream.max(Double::compare).get();
            } else {
                return stream.min(Double::compare).get();
            }
        }
    }
}
