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
import { SemanticLayerModal } from '../components/modals/SemanticLayerModal';
import { gotoWithRetry } from '../helpers/navigation';
import { URL } from '../utils/urls';

/**
 * Database Connections page object. With SEMANTIC_LAYERS on, its "New"
 * button is a menu offering a database or a semantic layer.
 */
export class DatabaseListPage {
  private readonly page: Page;

  private static readonly SELECTORS = {
    NEW_BUTTON: 'btn-create-new',
    NEW_SEMANTIC_LAYER_ITEM: 'Semantic Layer',
  } as const;

  constructor(page: Page) {
    this.page = page;
  }

  async goto(): Promise<void> {
    await gotoWithRetry(this.page, URL.DATABASE_LIST);
  }

  /** Opens "New" → "Semantic Layer" and hands back the dialog. */
  async startNewSemanticLayer(): Promise<SemanticLayerModal> {
    await this.page.getByTestId(DatabaseListPage.SELECTORS.NEW_BUTTON).click();
    await this.page
      .getByRole('menuitem', {
        name: DatabaseListPage.SELECTORS.NEW_SEMANTIC_LAYER_ITEM,
      })
      .click();
    const modal = new SemanticLayerModal(this.page);
    await modal.waitForVisible();
    return modal;
  }
}
