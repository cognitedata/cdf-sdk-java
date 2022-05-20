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

public enum ResourceType {
  ASSET,
  SEQUENCE_HEADER,
  SEQUENCE_BODY,
  THREED_MODEL_HEADER,
  TIMESERIES_HEADER,
  TIMESERIES_DATAPOINTS,
  EVENT,
  FILE_HEADER,
  FILE,
  RAW_DB,
  RAW_TABLE,
  RAW_ROW,
  DATA_SET,
  RELATIONSHIP,
  LABEL,
  SECURITY_CATEGORY,
  EXTRACTION_PIPELINE,
  EXTRACTION_PIPELINE_RUN,
  THREED_MODEL,
  THREED_MODEL_REVISION,
  THREED_NODE,
  THREED_NODE_FILTER,
  THREED_ANCESTOR_NODE,
  THREED_ASSET_MAPPINGS,
  TRANSFORMATIONS,
  TRANSFORMATIONS_JOBS,
  TRANSFORMATIONS_JOB_METRICS,
  TRANSFORMATIONS_SCHEDULES;
}
