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
 * Executable binding of playwright/acceptance/semantic-layer-setup.feature.
 *
 * Each step below is one line of that file. The steps only use the DSL
 * (playwright/acceptance/dsl), so nothing here says how a step is carried
 * out; the fixture decides whether the browser or the API does it.
 */

import { test, expect, acceptanceDriver } from '../../acceptance/fixtures';
import {
  SEMANTIC_LAYER_TYPE,
  SEMANTIC_VIEW_NAME,
} from '../../acceptance/environment';
import { skipUnlessFeatureEnabled } from '../../helpers/featureFlags';

test.beforeEach(async ({ page }) => {
  // Background: Given the semantic-layers feature is on
  await skipUnlessFeatureEnabled(page, 'SEMANTIC_LAYERS');
});

test('Admin connects a semantic layer and adds a view', async ({
  semanticLayers,
}) => {
  const layerName = `Sales (acceptance ${Date.now()})`;

  await test.step(`Given a "${SEMANTIC_LAYER_TYPE}" provider is installed`, async () => {
    const types = await semanticLayers.availableSemanticLayerTypes();
    test.skip(
      !types.includes(SEMANTIC_LAYER_TYPE),
      `No "${SEMANTIC_LAYER_TYPE}" provider is installed on this server`,
    );
  });

  await test.step(`When an admin connects a semantic layer of that type named "${layerName}"`, async () => {
    await semanticLayers.connectSemanticLayer({
      type: SEMANTIC_LAYER_TYPE,
      name: layerName,
    });
  });

  await test.step(`And adds the semantic view "${SEMANTIC_VIEW_NAME}" from it`, async () => {
    await semanticLayers.addSemanticView({
      name: SEMANTIC_VIEW_NAME,
      fromLayer: layerName,
    });
  });

  await test.step(`Then "${SEMANTIC_VIEW_NAME}" is listed among the datasets`, async () => {
    expect(await semanticLayers.isSemanticViewListed(SEMANTIC_VIEW_NAME)).toBe(
      true,
    );
  });

  await test.step(`And "${SEMANTIC_VIEW_NAME}" can be opened in Explore`, async () => {
    const explore =
      await semanticLayers.openSemanticViewInExplore(SEMANTIC_VIEW_NAME);
    expect(explore.datasetShown).toContain(SEMANTIC_VIEW_NAME);
  });
});

test('Admin is told to create a semantic layer before adding a view', async ({
  semanticLayers,
}) => {
  test.skip(
    acceptanceDriver === 'api',
    'Guidance is something a person sees; the API has no equivalent step.',
  );

  await test.step('Given no semantic layer is connected', async () => {
    const layers = await semanticLayers.connectedSemanticLayers();
    test.skip(
      layers.length > 0,
      `This server already has semantic layers (${layers.join(', ')}); the scenario needs none.`,
    );
  });

  const guidance =
    await test.step('When an admin starts adding a semantic view', () =>
      semanticLayers.startAddingSemanticView());

  await test.step('Then they are told a semantic layer must exist first', async () => {
    expect(guidance.explainsLayerPrerequisite).toBe(true);
  });

  await test.step('And they are offered a way to create one', async () => {
    expect(guidance.offersLayerSetup).toBe(true);
  });
});
