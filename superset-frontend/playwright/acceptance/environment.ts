/**
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

/**
 * What the test environment provides, keyed by product-level names.
 *
 * Scenarios say "a Snowflake Semantic Layer" and "the view SALES"; this
 * module says where such a thing can be found in the environment the tests
 * run against. It is the only place connection details live, so neither the
 * scenarios nor the DSL ever carry them.
 *
 * The proof of concept runs against SnuffLake, a local Snowflake emulator
 * that accepts any user, password and account. Nothing here is a real
 * credential. The Superset under test routes the account identifier
 * `snufflake` to the emulator; see the result write-up for that wiring.
 */

export interface SemanticLayerConnection {
  /** The provider type's product name, as `availableSemanticLayerTypes` reports it. */
  type: string;
  /** The provider's connection form, field by field, as the API accepts it. */
  configuration: Record<string, unknown>;
}

const env = (name: string, fallback: string): string =>
  process.env[name] ?? fallback;

/** The provider type the scenarios use. */
export const SEMANTIC_LAYER_TYPE = env(
  'ACCEPTANCE_SEMANTIC_LAYER_TYPE',
  'Snowflake Semantic Layer',
);

/** A view that provider offers in the test environment. */
export const SEMANTIC_VIEW_NAME = env('ACCEPTANCE_SEMANTIC_VIEW', 'SALES');

const CONNECTIONS: Record<string, SemanticLayerConnection> = {
  'Snowflake Semantic Layer': {
    type: 'Snowflake Semantic Layer',
    configuration: {
      account_identifier: env('ACCEPTANCE_SNOWFLAKE_ACCOUNT', 'snufflake'),
      auth: {
        auth_type: 'user_password',
        username: env('ACCEPTANCE_SNOWFLAKE_USER', 'fake'),
        password: env('ACCEPTANCE_SNOWFLAKE_PASSWORD', 'snow'),
      },
      database: env('ACCEPTANCE_SNOWFLAKE_DATABASE', 'SNUFFLAKE_GOLDEN'),
      allow_changing_database: false,
      schema: env('ACCEPTANCE_SNOWFLAKE_SCHEMA', 'SPIKE'),
      allow_changing_schema: false,
    },
  },
};

/** Connection details for a provider type, or a clear error naming the gap. */
export function connectionFor(type: string): SemanticLayerConnection {
  const connection = CONNECTIONS[type];
  if (!connection) {
    throw new Error(
      `The test environment has no connection details for semantic layer type "${type}". ` +
        `Known types: ${Object.keys(CONNECTIONS).join(', ')}`,
    );
  }
  return connection;
}
