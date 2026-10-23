Feature: Semantic layer setup
  An admin connects Superset to a semantic layer and makes its views
  available to analysts as datasets.

  This file is the specification: product language only. Each scenario is
  bound, step for step, to acceptance/dsl/semanticLayerSetup.ts in
  tests/acceptance/semantic-layer-setup.spec.ts. It names no screens,
  selectors, routes or ids, so it reads the same whether the steps are
  carried out through the browser or through the API.

  Background:
    Given the semantic-layers feature is on

  Scenario: Admin connects a semantic layer and adds a view
    Given a "Snowflake Semantic Layer" provider is installed
    When an admin connects a semantic layer of that type named "Sales (acceptance)"
    And adds the semantic view "SALES" from it
    Then "SALES" is listed among the datasets
    And "SALES" can be opened in Explore

  Scenario: Admin is told to create a semantic layer before adding a view
    Given no semantic layer is connected
    When an admin starts adding a semantic view
    Then they are told a semantic layer must exist first
    And they are offered a way to create one
