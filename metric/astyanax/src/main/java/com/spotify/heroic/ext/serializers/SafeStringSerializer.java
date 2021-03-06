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

package com.spotify.heroic.ext.serializers;

import java.nio.ByteBuffer;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.spotify.heroic.ext.marshal.SafeUTF8Type;

/**
 * A StringSerializer that correctly handles null and empty values.
 *
 * @author udoprog
 */
public class SafeStringSerializer extends AbstractSerializer<String> {
    private static final SafeStringSerializer instance = new SafeStringSerializer();

    public static SafeStringSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(String obj) {
        return SafeUTF8Type.INSTANCE.decompose(obj);
    }

    @Override
    public String fromByteBuffer(ByteBuffer byteBuffer) {
        return SafeUTF8Type.INSTANCE.compose(byteBuffer);
    }
}
