/*
 * Copyright (c) 2020 Cognite AS
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

package com.cognite.client.config;

/**
 * Sets the upsert mode of the writer.
 */
public enum UpsertMode {
    /**
     * Will insert if the data object does not exist. If the data object already exists, then only the provided field values
     * will overwrite the existing ones. Metadata fields are overwritten on a per key basis. AssetId(s) are completely replaced.
     */
    UPDATE,

    /**
     * Will insert if the data object does not exist. If the data object already exists, then it will be completely replaced
     * by the new object.
     */
    REPLACE
}
