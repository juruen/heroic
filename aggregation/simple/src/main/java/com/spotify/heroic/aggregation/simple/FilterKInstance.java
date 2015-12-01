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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationResult;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.aggregation.AggregationTraversal;
import com.spotify.heroic.aggregation.EmptyInstance;
import com.spotify.heroic.aggregation.ReducerSession;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import lombok.Data;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FilterKInstance implements AggregationInstance {
    public long getK() {
        return k;
    }

    public enum FilterType {
        TOP,
        BOTTOM
    }

    private final long k;
    private final FilterType filterType;

    public FilterKInstance(long k, FilterType filterType) {
        this.k = k;
        this.filterType = filterType;
    }

    @Override
    public long estimate(DateRange range) {
        return 0;
    }

    @Override
    public long cadence() {
        return -1;
    }

    @Override
    public AggregationTraversal session(List<AggregationState> states, DateRange range) {
        return new AggregationTraversal(states, new Session(states, range, k, filterType));
    }

    @Override
    public ReducerSession reducer(DateRange range) {
        return null;
    }

    private static class Session implements AggregationSession {
        private final long k;
        private final FilterType filterType;
        private final ImmutableMap<Map<String, String>, AggregationTraversal> groupToTraversal;

        public Session(List<AggregationState> states,
                       DateRange range,
                       long k,
                       FilterType filterType) {
            this.k = k;
            this.filterType = filterType;
            groupToTraversal = buildGroupToTraversal(states, range);
        }

        @Override
        public void updatePoints(Map<String, String> group, Set<Series> series,
                                 List<Point> values) {
            groupSession(group).updatePoints(group, series, values);
        }

        @Override
        public void updateEvents(Map<String, String> group, Set<Series> series,
                                 List<Event> values) {
            groupSession(group).updateEvents(group, series, values);
        }

        @Override
        public void updateSpreads(Map<String, String> group, Set<Series> series,
                                  List<Spread> values) {
            groupSession(group).updateSpreads(group, series, values);
        }

        @Override
        public void updateGroup(Map<String, String> group, Set<Series> series,
                                List<MetricGroup> values) {
            groupSession(group).updateGroup(group, series, values);
        }

        @Override
        public AggregationResult result() {
            final List<AggregationData> result = getAreaAggregations().stream()
                .sorted(this::compare)
                .limit(k)
                .map(AreaAggregation::getAggregationData)
                .collect(Collectors.toList());

            return new AggregationResult(result, Statistics.empty());
        }

        private int compare(AreaAggregation a, AreaAggregation b) {
            final Comparator<Double> comparator = Double::compare;

            if (filterType == FilterType.TOP) {
                return comparator.reversed().compare(a.getValue(), b.getValue());
            } else {
                return comparator.compare(a.getValue(), b.getValue());
            }
        }

        private ImmutableMap<Map<String, String>, AggregationTraversal> buildGroupToTraversal(
            List<AggregationState> states, DateRange range) {
            final EmptyInstance empty = new EmptyInstance();

            final ImmutableMap.Builder<Map<String, String>, AggregationTraversal> m =
                ImmutableMap.builder();

            states.stream()
                .forEach(s -> m.put(s.getKey(), empty.session(ImmutableList.of(s), range)));

            return m.build();
        }

        private AggregationSession groupSession(Map<String, String> group) {
            return groupToTraversal.get(group).getSession();
        }

        private List<AreaAggregation> getAreaAggregations() {
            return groupToTraversal.entrySet()
                .stream()
                .map(this::toAreaAggregation)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        }

        private List<AreaAggregation> toAreaAggregation(
            Map.Entry<Map<String, String>, AggregationTraversal> kv) {
            final Map<String, String> group = kv.getKey();
            final List<AggregationData> results = kv.getValue().getSession().result().getResult();

            return results.stream()
                .map(d -> new AreaAggregation(group, d))
                .collect(Collectors.toList());
        }
    }

    @Data
    private static class AreaAggregation {
        private final AggregationData aggregationData;
        private final double value;

        public AreaAggregation(Map<String, String> group, AggregationData data) {
            this.aggregationData = new AggregationData(group, data.getSeries(), data.getMetrics());
            this.value = computeArea(data);
        }

        private Double computeArea(AggregationData data) {
            /* For now, it assumes the base of the rectangle is one, i.e: the previous aggregation
               returns the same aggregation interval for every time series.
             */
            return data.getMetrics().getDataAs(Point.class)
                .stream()
                .map(Point::getValue)
                .reduce(0D, (a, b) -> a + b);
        }
    }
}
