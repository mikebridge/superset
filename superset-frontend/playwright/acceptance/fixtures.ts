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

import { test as base } from '@playwright/test';
import { SemanticLayerSetup } from './dsl/semanticLayerSetup';
import { SemanticLayerSetupApiDriver } from './drivers/api/SemanticLayerSetupApiDriver';
import { SemanticLayerSetupUiDriver } from './drivers/ui/SemanticLayerSetupUiDriver';

/** Which driver carries out the steps. `ACCEPTANCE_DRIVER=api` picks the API; the browser is the default. */
export type AcceptanceDriver = 'ui' | 'api';

export const acceptanceDriver: AcceptanceDriver =
  process.env.ACCEPTANCE_DRIVER === 'api' ? 'api' : 'ui';

/**
 * Acceptance-test fixtures.
 *
 * A scenario asks for `semanticLayers` and gets the DSL, typed as the
 * interface only. It cannot reach the page, a selector or an endpoint
 * through it, and it never names the driver, so the same scenario runs
 * through the browser or the API by flipping one environment variable.
 * Whatever the driver created is removed after the test.
 */
export const test = base.extend<{ semanticLayers: SemanticLayerSetup }>({
  semanticLayers: async ({ page }, provide) => {
    const driver: SemanticLayerSetup =
      acceptanceDriver === 'api'
        ? new SemanticLayerSetupApiDriver(page)
        : new SemanticLayerSetupUiDriver(page);
    await provide(driver);
    await driver.cleanUp();
  },
});

export { expect } from '@playwright/test';
