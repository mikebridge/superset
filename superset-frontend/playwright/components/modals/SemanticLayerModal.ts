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
 * The "New Semantic Layer" dialog (src/features/semanticLayers/SemanticLayerModal).
 *
 * Two steps: pick a type, then name the layer and fill the provider's own
 * configuration form. That form is rendered from the provider's JSON schema,
 * so its fields are addressed by their labels rather than by test ids.
 */
export class SemanticLayerModal extends Modal {
  private static readonly LABELS = {
    TYPE_SELECT: 'Semantic layer type',
    NAME_PLACEHOLDER: 'Name of the semantic layer',
    CREATE_BUTTON: 'Create',
  } as const;

  async waitForVisible(): Promise<void> {
    await this.element.waitFor({
      state: 'visible',
      timeout: TIMEOUT.FORM_LOAD,
    });
  }

  /** Options offered in the type step. */
  async availableTypes(): Promise<string[]> {
    const select = Select.fromRole(
      this.page,
      SemanticLayerModal.LABELS.TYPE_SELECT,
    );
    await select.open();
    const types = await select.getVisibleOptionTexts();
    await select.close();
    return types;
  }

  /** Picks a type and moves on to the configuration step. */
  async chooseType(typeName: string): Promise<void> {
    await Select.fromRole(
      this.page,
      SemanticLayerModal.LABELS.TYPE_SELECT,
    ).selectOption(typeName);
    // The type step's footer button carries the dialog's default label ("Add").
    await this.footer.getByRole('button', { name: /^(Add|Next)$/ }).click();
    await this.nameInput.waitFor({ state: 'visible' });
  }

  get nameInput(): Locator {
    return this.element.getByPlaceholder(
      SemanticLayerModal.LABELS.NAME_PLACEHOLDER,
    );
  }

  async fillName(name: string): Promise<void> {
    await this.nameInput.fill(name);
  }

  /** A text field of the provider's configuration form, by its label. */
  async fillField(label: string, value: string): Promise<void> {
    await this.element.getByLabel(label, { exact: true }).fill(value);
  }

  /**
   * Waits for the form to finish refreshing the fields it discovers from the
   * connection (the provider re-fetches its schema shortly after a dependency
   * such as the account or credentials changes, and those fields show a
   * loading indicator meanwhile).
   */
  async waitForDiscovery(): Promise<void> {
    const loading = this.element.locator('.ant-select-arrow-loading');
    await loading
      .first()
      .waitFor({ state: 'visible', timeout: TIMEOUT.CONFIRM_DIALOG })
      .catch(() => undefined);
    await loading
      .first()
      .waitFor({ state: 'hidden', timeout: TIMEOUT.API_RESPONSE })
      .catch(() => undefined);
  }

  /**
   * A field whose choices the form discovers from the connection, such as the
   * database. Rendered as a picker once choices arrived, or as a text field
   * when there are none yet; either way the value is set after discovery.
   */
  async setDiscoveredField(label: string, value: string): Promise<void> {
    await this.waitForDiscovery();
    const picker = this.element.getByRole('combobox', {
      name: label,
      exact: true,
    });
    if ((await picker.count()) > 0) {
      await Select.fromRole(this.page, label).selectOption(value);
    } else {
      await this.fillField(label, value);
    }
  }

  /**
   * A choice between variants of a configuration section (for example the
   * authentication method), rendered as radio buttons named after the variants.
   */
  async chooseVariant(option: string): Promise<void> {
    await this.element
      .getByRole('radio', { name: option, exact: true })
      .check();
  }

  async clickCreate(): Promise<void> {
    await this.footer
      .getByRole('button', { name: SemanticLayerModal.LABELS.CREATE_BUTTON })
      .click();
  }

  async waitForClose(): Promise<void> {
    await this.element.waitFor({
      state: 'hidden',
      timeout: TIMEOUT.API_RESPONSE,
    });
  }
}
