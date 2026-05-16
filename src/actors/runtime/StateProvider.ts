/*
Copyright 2022 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import { OperationType } from "../../types/Operation.type";
import ActorId from "../ActorId";
import ActorClient from "../client/ActorClient/ActorClient";
import ActorStateChange from "./ActorStateChange";
import StateChangeKind from "./StateChangeKind";

/**
 * Persistence abstraction for actor state using the Dapr state store.
 *
 * StateProvider encapsulates the contract for durable state operations, delegating
 * to the ActorClient to interact with the Dapr sidecar. All state mutations are
 * persisted transactionally through the Dapr runtime, ensuring consistency.
 *
 * @remarks
 * The provider supports three core operations:
 * - Check if a state value exists in the store
 * - Load existing state values (returns a tuple for option-like semantics)
 * - Persist batches of state changes atomically via transactional writes
 *
 * State changes are mapped to Dapr operations:
 * - ADD/UPDATE both map to "upsert" (Dapr has no separate add operation)
 * - REMOVE maps to "delete"
 * - NONE changes are skipped
 *
 * @internal
 */
export default class StateProvider {
  /**
   * The actor client used to communicate with the Dapr runtime.
   */
  actorClient: ActorClient;

  /**
   * Constructs a StateProvider instance.
   *
   * @param actorClient - The ActorClient for Dapr sidecar communication.
   */
  constructor(actorClient: ActorClient) {
    this.actorClient = actorClient;
  }

  /**
   * Checks whether a state value exists for the given actor and state name.
   *
   * @param actorType - The actor type identifier.
   * @param actorId - The actor instance identifier.
   * @param stateName - The name of the state to check.
   * @returns True if the state exists and has non-empty value; false otherwise.
   */
  async containsState(actorType: string, actorId: ActorId, stateName: string): Promise<boolean> {
    const rawStateValue = await this.actorClient.actor.stateGet(actorType, actorId, stateName);
    return !!rawStateValue && rawStateValue.length > 0;
  }

  // SEE https://github.com/dapr/python-sdk/blob/0f0b6f6a1cf45d2ac0c519b48fc868898d81124e/dapr/actor/runtime/_state_provider.py#L24
  /**
   * Attempts to load state from the Dapr state store.
   *
   * Returns a tuple [found, value] using option-like semantics. If the state exists,
   * found is true and value contains the loaded data. If not found, found is false
   * and value is undefined. Empty values are treated as not found.
   *
   * @param actorType - The actor type identifier.
   * @param actorId - The actor instance identifier.
   * @param stateName - The name of the state to load.
   * @returns A tuple [boolean, any] where boolean indicates if state was found.
   */
  async tryLoadState(actorType: string, actorId: ActorId, stateName: string): Promise<[boolean, any]> {
    const rawStateValue = await this.actorClient.actor.stateGet(actorType, actorId, stateName);

    if (!rawStateValue || rawStateValue.length === 0) {
      return [false, undefined];
    }

    // const result = this.serializer.deserialize(rawStateValue);

    return [true, rawStateValue];
  }

  /**
   * Persists a batch of state changes transactionally to the Dapr state store.
   *
   * All changes are sent in a single transactional request, ensuring atomicity.
   * State changes are mapped to Dapr operations: ADD/UPDATE become "upsert",
   * REMOVE becomes "delete", and NONE changes are omitted.
   *
   * See Dapr's transactional state request documentation:
   * https://github.com/dapr/dapr/blob/master/pkg/actors/transactional_state_request.go
   *
   * Example request body format:
   * ```json
   * [
   *   { "operation": "upsert", "request": { "key": "key1", "value": "myData" } },
   *   { "operation": "delete", "request": { "key": "key2" } }
   * ]
   * ```
   *
   * @param actorType - The actor type identifier.
   * @param actorId - The actor instance identifier.
   * @param stateChanges - Array of state mutations to persist atomically.
   * @throws May throw if the transactional write to Dapr fails.
   */
  async saveState(actorType: string, actorId: ActorId, stateChanges: ActorStateChange<any>[]): Promise<void> {
    const jsonOutput: OperationType[] = [];

    for (const state of stateChanges) {
      let operation;

      switch (state.getChangeKind()) {
        case StateChangeKind.ADD:
          // dapr doesn't know add, we use upsert
          // https://github.com/dapr/dapr/blob/master/pkg/actors/transactional_state_request.go
          operation = "upsert";
          break;
        case StateChangeKind.REMOVE:
          operation = "delete";
          break;
        case StateChangeKind.UPDATE:
          // dapr doesn't know add, we use upsert
          // https://github.com/dapr/dapr/blob/master/pkg/actors/transactional_state_request.go
          operation = "upsert";
          break;
        default:
          // skip
          continue;
      }

      jsonOutput.push({
        operation,
        request: {
          key: state.getStateName(),
          value: state.getValue() as string,
        },
      });
    }

    await this.actorClient.actor.stateTransaction(actorType, actorId, jsonOutput);
  }
}
