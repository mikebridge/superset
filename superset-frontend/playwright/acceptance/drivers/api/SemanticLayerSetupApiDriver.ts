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

import { Page } from '@playwright/test';
import {
  apiDeleteSemanticLayer,
  apiDeleteSemanticView,
  apiGetAvailableSemanticViews,
  apiGetExploreDataset,
  apiGetSemanticLayerTypes,
  apiGetSemanticLayers,
  apiGetSemanticViewsByName,
  apiPostSemanticLayer,
  apiPostSemanticViews,
} from '../../../helpers/api/semanticLayer';
import { connectionFor } from '../../environment';
import {
  AddSemanticViewGuidance,
  ExploreOutcome,
  SemanticLayerSetup,
  SemanticLayerToConnect,
  SemanticViewToAdd,
} from '../../dsl/semanticLayerSetup';

/**
 * Carries out the semantic layer setup vocabulary through the REST API.
 *
 * Same interface as the UI driver, so the same scenario runs either way.
 * The API has no screens, so a step whose outcome is something a person
 * reads (guidance in a dialog) has no equivalent here and says so.
 */
export class SemanticLayerSetupApiDriver implements SemanticLayerSetup {
  private readonly createdLayerUuids: string[] = [];

  private readonly createdViewIds: number[] = [];

  constructor(private readonly page: Page) {}

  async availableSemanticLayerTypes(): Promise<string[]> {
    const types = await apiGetSemanticLayerTypes(this.page);
    return types.map(type => type.name);
  }

  async connectedSemanticLayers(): Promise<string[]> {
    const layers = await apiGetSemanticLayers(this.page);
    return layers.map(layer => layer.name);
  }

  async connectSemanticLayer(layer: SemanticLayerToConnect): Promise<void> {
    const connection = connectionFor(layer.type);
    const types = await apiGetSemanticLayerTypes(this.page);
    const type = types.find(candidate => candidate.name === layer.type);
    if (!type) {
      throw new Error(
        `No semantic layer type named "${layer.type}" is installed`,
      );
    }
    const uuid = await apiPostSemanticLayer(this.page, {
      name: layer.name,
      type: type.id,
      configuration: connection.configuration,
    });
    this.createdLayerUuids.push(uuid);
  }

  async addSemanticView(view: SemanticViewToAdd): Promise<void> {
    const layer = (await apiGetSemanticLayers(this.page)).find(
      candidate => candidate.name === view.fromLayer,
    );
    if (!layer) {
      throw new Error(
        `No semantic layer named "${view.fromLayer}" is connected`,
      );
    }
    const offered = await apiGetAvailableSemanticViews(this.page, layer.uuid);
    if (!offered.some(candidate => candidate.name === view.name)) {
      throw new Error(
        `"${view.fromLayer}" does not offer a view named "${view.name}"; ` +
          `it offers: ${offered.map(candidate => candidate.name).join(', ')}`,
      );
    }
    const result = await apiPostSemanticViews(this.page, [
      { name: view.name, semantic_layer_uuid: layer.uuid, configuration: {} },
    ]);
    if (result.errors?.length) {
      throw new Error(
        `Adding "${view.name}" failed: ${JSON.stringify(result.errors)}`,
      );
    }
    const created = await apiGetSemanticViewsByName(this.page, view.name);
    this.createdViewIds.push(...created.map(candidate => candidate.id));
  }

  async isSemanticViewListed(name: string): Promise<boolean> {
    const views = await apiGetSemanticViewsByName(this.page, name);
    return views.length > 0;
  }

  async openSemanticViewInExplore(name: string): Promise<ExploreOutcome> {
    const [view] = await apiGetSemanticViewsByName(this.page, name);
    if (!view) {
      throw new Error(`No semantic view named "${name}" is listed`);
    }
    const dataset = await apiGetExploreDataset(
      this.page,
      'semantic_view',
      view.id,
    );
    return { datasetShown: dataset.name };
  }

  async startAddingSemanticView(): Promise<AddSemanticViewGuidance> {
    throw new Error(
      'Guidance is something a person sees in the Add Semantic View dialog; the API has no equivalent.',
    );
  }

  async cleanUp(): Promise<void> {
    for (const id of this.createdViewIds) {
      await apiDeleteSemanticView(this.page, id, { failOnStatusCode: false });
    }
    for (const uuid of this.createdLayerUuids) {
      await apiDeleteSemanticLayer(this.page, uuid, {
        failOnStatusCode: false,
      });
    }
  }
}
