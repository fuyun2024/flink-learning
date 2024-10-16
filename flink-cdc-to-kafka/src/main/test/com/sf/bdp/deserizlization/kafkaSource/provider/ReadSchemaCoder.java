/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sf.bdp.deserizlization.kafkaSource.provider;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;


public interface ReadSchemaCoder {

    Schema readSchema(InputStream in) throws IOException;


    /**
     * Provider for {@link ReadSchemaCoder}. It allows creating multiple instances of client in parallel
     * operators without serializing it.
     */
    interface SchemaCoderProvider extends Serializable {

        /**
         * Creates a new instance of {@link ReadSchemaCoder}. Each time it should create a new instance,
         * as it will be called on multiple nodes.
         *
         * @return new instance {@link ReadSchemaCoder}
         */
        ReadSchemaCoder get();
    }
}
