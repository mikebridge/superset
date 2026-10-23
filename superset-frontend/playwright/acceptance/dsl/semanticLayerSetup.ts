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
 * The domain-specific language (DSL) for semantic layer setup.
 *
 * This is the vocabulary acceptance scenarios are written in. It speaks in
 * product terms only: types, names and outcomes a user would recognise.
 * It never mentions ids, selectors, routes, payloads or any other detail of
 * how a step is carried out. Those belong to the drivers, which implement
 * this interface once for the browser (drivers/ui) and once for the HTTP API
 * (drivers/api). A scenario compiles against this interface, so a step that
 * leaks implementation detail is a type error, not a review comment.
 *
 * Writing a new scenario starts here: add the verb or outcome it needs, let
 * the compiler point at the drivers that must implement it, then write the
 * scenario. It is red until the product does what the scenario says.
 */

/** A semantic layer as an admin would describe it. */
export interface SemanticLayerToConnect {
  /** The provider type as the product names it, e.g. "Snowflake Semantic Layer". */
  type: string;
  /** The name the admin gives the connection. */
  name: string;
}

/** A semantic view as an admin would describe it. */
export interface SemanticViewToAdd {
  /** The view's name as the provider lists it. */
  name: string;
  /** The name of the connected semantic layer that offers it. */
  fromLayer: string;
}

/** What an admin sees after opening Explore on a semantic view. */
export interface ExploreOutcome {
  /** The dataset Explore says it is showing. */
  datasetShown: string;
}

/**
 * What an admin is told when they start adding a semantic view.
 *
 * Both flags are false when the product offers no guidance at all, which is
 * the state this proof of concept shows as red on master.
 */
export interface AddSemanticViewGuidance {
  /** The user is told that a semantic layer must exist before a view can be added. */
  explainsLayerPrerequisite: boolean;
  /** The user is offered a way to create a semantic layer from where they are. */
  offersLayerSetup: boolean;
}

export interface SemanticLayerSetup {
  /** The semantic-layer provider types this server offers, by product name. */
  availableSemanticLayerTypes(): Promise<string[]>;

  /** The names of the semantic layers connected and visible to the acting user. */
  connectedSemanticLayers(): Promise<string[]>;

  /**
   * Connect a semantic layer. Connection details for the type come from the
   * test environment (acceptance/environment.ts); the scenario never sees them.
   */
  connectSemanticLayer(layer: SemanticLayerToConnect): Promise<void>;

  /** Add one of the views a connected layer offers, making it a dataset. */
  addSemanticView(view: SemanticViewToAdd): Promise<void>;

  /** Whether a semantic view appears among the datasets. */
  isSemanticViewListed(name: string): Promise<boolean>;

  /** Open a listed semantic view in Explore and report what Explore shows. */
  openSemanticViewInExplore(name: string): Promise<ExploreOutcome>;

  /** Begin adding a semantic view and report the guidance offered, without adding anything. */
  startAddingSemanticView(): Promise<AddSemanticViewGuidance>;

  /** Housekeeping, not a product step: remove everything this driver created. */
  cleanUp(): Promise<void>;
}
