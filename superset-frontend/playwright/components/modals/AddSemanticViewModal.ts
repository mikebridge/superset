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

import { Locator } from '@playwright/test';
import { Modal, Select } from '../core';
import { TIMEOUT } from '../../utils/constants';

/**
 * The "Add Semantic View" dialog (src/features/semanticViews/AddSemanticViewModal).
 */
export class AddSemanticViewModal extends Modal {
  private static readonly LABELS = {
    LAYER_SELECT: 'Semantic layer',
    VIEWS_SELECT: 'Semantic views',
    ADD_BUTTON: /^Add/,
    /** Draft copy from the setup-guidance work; the driver owns this, not the scenario. */
    PREREQUISITE_TEXT: /create a semantic layer before adding a semantic view/i,
    CREATE_LAYER_BUTTON: 'Create semantic layer',
  } as const;

  async waitForVisible(): Promise<void> {
    await this.element.waitFor({
      state: 'visible',
      timeout: TIMEOUT.FORM_LOAD,
    });
  }

  /** Waits until the dialog has finished loading the layers it can offer. */
  async waitForLayersLoaded(): Promise<void> {
    await this.page
      .locator('.ant-select-arrow-loading')
      .waitFor({ state: 'hidden', timeout: TIMEOUT.API_RESPONSE })
      .catch(() => undefined);
  }

  private get layerSelect(): Select {
    return Select.fromRole(this.page, AddSemanticViewModal.LABELS.LAYER_SELECT);
  }

  /** The layers the dialog offers; empty when none is available to this user. */
  async offeredLayers(): Promise<string[]> {
    await this.waitForLayersLoaded();
    const select = this.layerSelect;
    if (await select.element.isDisabled()) return [];
    await select.open();
    // An empty picker shows Ant Design's "No data" placeholder, which is not
    // an option, so read whatever options exist rather than waiting for one.
    await this.page.getByRole('listbox').last().waitFor({ state: 'visible' });
    const options = (await select.options.allTextContents()).map(text =>
      text.trim(),
    );
    await select.close();
    return options;
  }

  async chooseLayer(layerName: string): Promise<void> {
    await this.waitForLayersLoaded();
    await this.layerSelect.selectOption(layerName);
  }

  async chooseView(viewName: string): Promise<void> {
    const select = Select.fromRole(
      this.page,
      AddSemanticViewModal.LABELS.VIEWS_SELECT,
    );
    await select.element.waitFor({ state: 'visible' });
    await select.selectOption(viewName);
  }

  async clickAdd(): Promise<void> {
    await this.footer
      .getByRole('button', { name: AddSemanticViewModal.LABELS.ADD_BUTTON })
      .click();
  }

  async waitForClose(): Promise<void> {
    await this.element.waitFor({
      state: 'hidden',
      timeout: TIMEOUT.API_RESPONSE,
    });
  }

  get prerequisiteExplanation(): Locator {
    return this.element.getByText(
      AddSemanticViewModal.LABELS.PREREQUISITE_TEXT,
    );
  }

  get createLayerButton(): Locator {
    return this.element.getByRole('button', {
      name: AddSemanticViewModal.LABELS.CREATE_LAYER_BUTTON,
    });
  }

  async close(): Promise<void> {
    await this.page.keyboard.press('Escape');
    await this.element
      .waitFor({ state: 'hidden', timeout: TIMEOUT.UI_TRANSITION })
      .catch(() => undefined);
  }
}
