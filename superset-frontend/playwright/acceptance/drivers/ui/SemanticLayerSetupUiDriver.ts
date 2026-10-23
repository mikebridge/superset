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
import { Toast } from '../../../components/core';
import { SemanticLayerModal } from '../../../components/modals/SemanticLayerModal';
import { DatabaseListPage } from '../../../pages/DatabaseListPage';
import { DatasetListPage } from '../../../pages/DatasetListPage';
import { ExplorePage } from '../../../pages/ExplorePage';
import { TIMEOUT } from '../../../utils/constants';
import {
  apiDeleteSemanticLayer,
  apiDeleteSemanticView,
  apiGetSemanticLayers,
  apiGetSemanticViewsByName,
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
 * Carries out the semantic layer setup vocabulary through the browser.
 *
 * This is the only layer that knows which screen a step happens on. It
 * composes the repository's page objects (playwright/pages, playwright/
 * components) and adds nothing about the product's meaning: that stays in
 * the DSL. When the UI changes, this file and the page objects change; the
 * scenarios do not.
 *
 * Reads go through the UI too, so an outcome is what a person would see.
 * The one exception is {@link cleanUp}, which is housekeeping rather than a
 * product step and uses the API like the repository's other fixtures.
 */
export class SemanticLayerSetupUiDriver implements SemanticLayerSetup {
  private readonly createdLayers: string[] = [];

  private readonly createdViews: string[] = [];

  constructor(private readonly page: Page) {}

  async availableSemanticLayerTypes(): Promise<string[]> {
    const databases = new DatabaseListPage(this.page);
    await databases.goto();
    const dialog = await databases.startNewSemanticLayer();
    const types = await dialog.availableTypes();
    await this.page.keyboard.press('Escape');
    return types;
  }

  async connectedSemanticLayers(): Promise<string[]> {
    const datasets = new DatasetListPage(this.page);
    await datasets.goto();
    const dialog = await datasets.startNewSemanticView();
    const layers = await dialog.offeredLayers();
    await dialog.close();
    return layers;
  }

  async connectSemanticLayer(layer: SemanticLayerToConnect): Promise<void> {
    const connection = connectionFor(layer.type);
    const databases = new DatabaseListPage(this.page);
    await databases.goto();
    const dialog = await databases.startNewSemanticLayer();
    await dialog.chooseType(layer.type);
    await dialog.fillName(layer.name);
    await this.fillProviderForm(dialog, connection.configuration);
    await dialog.clickCreate();
    await new Toast(this.page)
      .getSuccess()
      .waitFor({ state: 'visible', timeout: TIMEOUT.API_RESPONSE });
    await dialog.waitForClose();
    this.createdLayers.push(layer.name);
  }

  async addSemanticView(view: SemanticViewToAdd): Promise<void> {
    const datasets = new DatasetListPage(this.page);
    await datasets.goto();
    const dialog = await datasets.startNewSemanticView();
    await dialog.chooseLayer(view.fromLayer);
    await dialog.chooseView(view.name);
    await dialog.clickAdd();
    await new Toast(this.page)
      .getSuccess()
      .waitFor({ state: 'visible', timeout: TIMEOUT.API_RESPONSE });
    await dialog.waitForClose();
    this.createdViews.push(view.name);
  }

  async isSemanticViewListed(name: string): Promise<boolean> {
    const datasets = new DatasetListPage(this.page);
    await datasets.goto();
    await datasets.waitForTableLoad();
    return datasets.getDatasetRow(name).isVisible();
  }

  async openSemanticViewInExplore(name: string): Promise<ExploreOutcome> {
    const datasets = new DatasetListPage(this.page);
    await datasets.goto();
    await datasets.waitForTableLoad();
    await datasets.clickDatasetName(name);
    const explore = new ExplorePage(this.page);
    await explore.waitForPageLoad({ timeout: TIMEOUT.EXPLORE_PAGE_LOAD });
    return { datasetShown: await explore.getDatasetName() };
  }

  async startAddingSemanticView(): Promise<AddSemanticViewGuidance> {
    const datasets = new DatasetListPage(this.page);
    await datasets.goto();
    const dialog = await datasets.startNewSemanticView();
    await dialog.waitForLayersLoaded();
    const guidance = {
      explainsLayerPrerequisite:
        await dialog.prerequisiteExplanation.isVisible(),
      offersLayerSetup: await dialog.createLayerButton.isVisible(),
    };
    await dialog.close();
    return guidance;
  }

  async cleanUp(): Promise<void> {
    for (const name of this.createdViews) {
      for (const view of await apiGetSemanticViewsByName(this.page, name)) {
        await apiDeleteSemanticView(this.page, view.id, {
          failOnStatusCode: false,
        });
      }
    }
    const layers = await apiGetSemanticLayers(this.page);
    for (const layer of layers.filter(l =>
      this.createdLayers.includes(l.name),
    )) {
      await apiDeleteSemanticLayer(this.page, layer.uuid, {
        failOnStatusCode: false,
      });
    }
  }

  /**
   * Fill the provider's configuration form from the environment's connection
   * details. The form is generated from the provider's schema, so fields are
   * found by label. Nested objects (an authentication method) are filled
   * field by field after choosing the method.
   */
  private async fillProviderForm(
    dialog: SemanticLayerModal,
    configuration: Record<string, unknown>,
  ): Promise<void> {
    const entries = Object.entries(configuration);
    const isDiscovered = ([field]: [string, unknown]) =>
      SemanticLayerSetupUiDriver.DISCOVERED_FIELDS.includes(field);
    for (const [field, value] of entries.filter(e => !isDiscovered(e))) {
      if (typeof value === 'boolean') continue; // defaults are kept
      if (value && typeof value === 'object') {
        const method = value as Record<string, unknown>;
        if (typeof method.auth_type === 'string') {
          await dialog.chooseVariant(
            SemanticLayerSetupUiDriver.AUTH_METHOD_LABELS[method.auth_type] ??
              method.auth_type,
          );
        }
        for (const [subField, subValue] of Object.entries(method)) {
          if (subField === 'auth_type') continue;
          await dialog.fillField(
            SemanticLayerSetupUiDriver.labelFor(subField),
            String(subValue),
          );
        }
      } else {
        await dialog.fillField(
          SemanticLayerSetupUiDriver.labelFor(field),
          String(value),
        );
      }
    }
    // Discovered fields are set last, one at a time, each after the form has
    // fetched its choices: the database list needs the account and
    // credentials, the schema list needs the database.
    for (const [field, value] of entries.filter(isDiscovered)) {
      await dialog.setDiscoveredField(
        SemanticLayerSetupUiDriver.labelFor(field),
        String(value),
      );
    }
    await dialog.waitForDiscovery();
  }

  /**
   * Fields whose choices the provider discovers from the connection
   * (`x-dynamic` in its schema), in dependency order.
   */
  private static readonly DISCOVERED_FIELDS = ['database', 'schema'];

  /** The provider's authentication methods as the form names them. */
  private static readonly AUTH_METHOD_LABELS: Record<string, string> = {
    user_password: 'Username and password',
    private_key: 'Private key',
  };

  /** Schema field name → form label (the schema titles its fields in Title Case). */
  private static labelFor(field: string): string {
    return field
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  }
}
