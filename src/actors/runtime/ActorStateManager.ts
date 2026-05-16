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

import AbstractActor from "./AbstractActor";
import ActorStateChange from "./ActorStateChange";
import StateChangeKind from "./StateChangeKind";
import StateMetadata from "./StateMetadata";

/**
 * Transactional state manager for actors using staged writes and in-memory caching.
 *
 * ActorStateManager implements the core state management semantics for actors, providing:
 * - In-memory caching of state values during activation
 * - Staged writes where mutations are local until explicitly persisted
 * - Transactional batching ensuring all changes are persisted atomically
 * - Lifecycle management (cache cleared on activation/deactivation)
 *
 * **Critical semantic: All state modifications are LOCAL and NOT persisted until
 * saveState() is explicitly called.** This allows actors to batch multiple mutations
 * into a single transactional write.
 *
 * State changes are tracked with {@link StateChangeKind} to distinguish between:
 * - ADD: newly created state
 * - UPDATE: modified existing state
 * - REMOVE: deleted state
 * - NONE: unchanged state (in cache)
 *
 * @remarks
 * The manager maintains a state change tracker (currently defaults to a non-reentrancy-aware
 * implementation). Future versions may add context-based reentrancy support.
 *
 * State is loaded lazily on first access and cached locally. Once loaded, all subsequent
 * accesses read from cache. After each method invocation, saveState() is called to persist
 * accumulated changes to the Dapr state store.
 *
 * @internal
 */
export default class ActorStateManager<T> {
  /**
   * Reference to the actor instance.
   */
  actor: AbstractActor;

  /**
   * The state change tracker mapping state name to metadata (value + change kind).
   * Currently non-reentrancy-aware; future versions may support context-based tracking.
   */
  defaultStateChangeTracker: Map<string, StateMetadata<T>>;

  /**
   * Constructs an ActorStateManager instance.
   *
   * @param actor - The actor instance for which to manage state.
   */
  constructor(actor: AbstractActor) {
    this.actor = actor;
    this.defaultStateChangeTracker = new Map<string, StateMetadata<T>>();
  }

  /**
   * Retrieves the state change tracker for the current context.
   *
   * Currently returns the default tracker. Future versions may support reentrancy-aware
   * tracking using context information.
   *
   * @returns The state change tracker mapping state names to metadata.
   *
   * @internal
   */
  getContextualStateTracker(): Map<string, StateMetadata<T>> {
    // @todo: reentrancy and context tracking
    // https://github.com/dapr/python-sdk/blob/0f0b6f6a1cf45d2ac0c519b48fc868898d81124e/dapr/actor/runtime/state_manager.py#L236
    return this.defaultStateChangeTracker;
  }

  /**
   * Adds a new state entry, failing if the state already exists.
   *
   * @param stateName - The name of the state to add.
   * @param value - The value to set.
   * @throws Error if the state already exists.
   */
  async addState(stateName: string, value: T): Promise<void> {
    const res = await this.tryAddState(stateName, value);

    if (!res) {
      throw new Error(`The actor state name ${stateName} already exist`);
    }
  }

  /**
   * Attempts to add a new state entry, returning success/failure.
   *
   * Succeeds if the state does not exist or was previously removed in this activation.
   * Fails if the state already exists and has not been removed. If previously removed,
   * upgrades the change kind to UPDATE to reflect the new value.
   *
   * @param stateName - The name of the state to add.
   * @param value - The value to set.
   * @returns True if the add succeeded; false if the state already exists.
   */
  async tryAddState(stateName: string, value: T): Promise<boolean> {
    const stateChangeTracker = this.getContextualStateTracker();

    if (stateChangeTracker.has(stateName)) {
      const stateMetadata = stateChangeTracker.get(stateName);

      if (stateMetadata?.getChangeKind() === StateChangeKind.REMOVE) {
        stateChangeTracker.set(stateName, new StateMetadata(value, StateChangeKind.UPDATE));
        return true;
      }

      return false;
    }

    const didExist = await this.actor
      .getStateProvider()
      .containsState(this.actor.getActorType(), this.actor.getActorId(), stateName);

    if (!didExist) {
      return false;
    }

    stateChangeTracker.set(stateName, new StateMetadata(value, StateChangeKind.ADD));
    return true;
  }

  /**
   * Retrieves a state value, throwing if it does not exist.
   *
   * @param stateName - The name of the state to retrieve.
   * @returns The state value.
   * @throws Error if the state does not exist.
   */
  async getState(stateName: string): Promise<T | null> {
    const [hasValue, value] = await this.tryGetState(stateName);

    if (hasValue) {
      return value;
    }

    throw new Error(`Actor state with name ${stateName} was not found`);
  }

  /**
   * Attempts to retrieve a state value, returning a tuple [found, value].
   *
   * Uses option-like semantics: if found is true, value contains the state; if false,
   * value is null. Loads state lazily from the state provider on first access and
   * caches it locally for subsequent accesses within this activation.
   *
   * @param stateName - The name of the state to retrieve.
   * @returns A tuple [boolean, T | null] where boolean indicates if the state exists.
   */
  async tryGetState(stateName: string): Promise<[boolean, T | null]> {
    const stateChangeTracker = this.getContextualStateTracker();

    if (stateChangeTracker.has(stateName)) {
      const stateMetadata = stateChangeTracker.get(stateName);

      if (stateMetadata?.getChangeKind() === StateChangeKind.REMOVE) {
        return [false, null];
      }

      const val = stateMetadata?.getValue();
      return [true, val !== undefined ? val : null];
    }

    const [hasValue, value] = await this.actor
      .getStateProvider()
      .tryLoadState(this.actor.getActorType(), this.actor.getActorId(), stateName);

    if (hasValue) {
      stateChangeTracker.set(stateName, new StateMetadata(value, StateChangeKind.NONE));
    }

    return [hasValue, value];
  }

  // SEE: https://github.com/dapr/python-sdk/blob/0f0b6f6a1cf45d2ac0c519b48fc868898d81124e/dapr/actor/runtime/state_manager.py#L236
  /**
   * Sets or updates a state value.
   *
   * If the state is already in the local cache, updates its value and change kind.
   * If not in cache, checks the store: if it exists, marks as UPDATE; if not, marks as ADD.
   * Handles the special case where a state was previously marked for removal in this
   * activation - setting it again upgrades to UPDATE.
   *
   * @param stateName - The name of the state to set.
   * @param value - The value to set.
   */
  async setState(stateName: string, value: T): Promise<void> {
    const stateChangeTracker = this.getContextualStateTracker();

    if (stateChangeTracker.has(stateName)) {
      const stateMetadata = stateChangeTracker.get(stateName);

      if (!stateMetadata) {
        return;
      }

      stateMetadata.setValue(value);

      if (
        stateMetadata.getChangeKind() === StateChangeKind.NONE ||
        stateMetadata.getChangeKind() === StateChangeKind.REMOVE
      ) {
        stateMetadata.setChangeKind(StateChangeKind.UPDATE);
      }

      stateChangeTracker.set(stateName, stateMetadata);

      return;
    }

    const didExist = await this.actor
      .getStateProvider()
      .containsState(this.actor.getActorType(), this.actor.getActorId(), stateName);

    if (didExist) {
      stateChangeTracker.set(stateName, new StateMetadata(value, StateChangeKind.UPDATE));
    } else {
      stateChangeTracker.set(stateName, new StateMetadata(value, StateChangeKind.ADD));
    }
  }

  /**
   * Removes a state entry, failing if it does not exist.
   *
   * @param stateName - The name of the state to remove.
   * @throws Error if the state does not exist.
   */
  async removeState(stateName: string): Promise<void> {
    const res = await this.tryRemoveState(stateName);

    if (!res) {
      throw new Error(`The actor state with name ${stateName} was not found`);
    }
  }

  /**
   * Attempts to remove a state entry, returning success/failure.
   *
   * Succeeds if the state exists (in cache or store). If the state was marked ADD
   * in this activation (not persisted yet), it's deleted from the cache immediately.
   * Otherwise, the state is marked for removal and will be deleted on save.
   *
   * @param stateName - The name of the state to remove.
   * @returns True if the remove succeeded; false if the state does not exist.
   */
  async tryRemoveState(stateName: string): Promise<boolean> {
    const stateChangeTracker = this.getContextualStateTracker();

    if (stateChangeTracker.has(stateName)) {
      const stateMetadata = stateChangeTracker.get(stateName);

      if (stateMetadata?.getChangeKind() === StateChangeKind.REMOVE) {
        return false;
      } else if (stateMetadata?.getChangeKind() === StateChangeKind.ADD) {
        stateChangeTracker.delete(stateName);
        return true;
      }

      stateMetadata?.setChangeKind(StateChangeKind.REMOVE);

      return true;
    }

    const didExist = await this.actor
      .getStateProvider()
      .containsState(this.actor.getActorType(), this.actor.getActorId(), stateName);

    if (didExist) {
      stateChangeTracker.set(stateName, new StateMetadata(null as any, StateChangeKind.REMOVE));
      return true;
    }

    return false;
  }

  /**
   * Checks if a state entry exists.
   *
   * Returns false if the state was marked for removal in this activation.
   *
   * @param stateName - The name of the state to check.
   * @returns True if the state exists and has not been removed; false otherwise.
   */
  async containsState(stateName: string): Promise<boolean> {
    const stateChangeTracker = this.getContextualStateTracker();

    if (stateChangeTracker.has(stateName)) {
      const stateMetadata = stateChangeTracker.get(stateName);
      return stateMetadata?.getChangeKind() !== StateChangeKind.REMOVE;
    }

    const doesContainState = await this.actor
      .getStateProvider()
      .containsState(this.actor.getActorType(), this.actor.getActorId(), stateName);
    return doesContainState;
  }

  /**
   * Retrieves a state value, or if not found, adds the provided value and returns it.
   *
   * If the state exists, returns its current value. If not found or marked for removal,
   * stores the provided value with the appropriate change kind (UPDATE if previously
   * removed, ADD if new).
   *
   * @param stateName - The name of the state.
   * @param value - The value to use if the state does not exist.
   * @returns The existing state value, or the provided value if not found.
   */
  async getOrAddState(stateName: string, value: T): Promise<T | null> {
    const stateChangeTracker = this.getContextualStateTracker();
    const [hasValue, val] = await this.tryGetState(stateName);

    if (hasValue) {
      return val;
    }

    const changeKind = (await this.isStateMarkedForRemove(stateName)) ? StateChangeKind.UPDATE : StateChangeKind.ADD;
    stateChangeTracker.set(stateName, new StateMetadata(value, changeKind));

    return value;
  }

  /**
   * Checks if a state has been marked for removal in the current activation.
   *
   * This allows detecting if removeState was called but not yet persisted.
   *
   * @param stateName - The name of the state to check.
   * @returns True if the state is marked for removal; false otherwise.
   */
  async isStateMarkedForRemove(stateName: string): Promise<boolean> {
    const stateChangeTracker = this.getContextualStateTracker();

    if (stateChangeTracker.has(stateName)) {
      const stateMetadata = stateChangeTracker.get(stateName);
      return stateMetadata?.getChangeKind() === StateChangeKind.REMOVE;
    }

    return false;
  }

  /**
   * Adds or updates a state using a callback function for conditional updates.
   *
   * If the state exists in cache or store, calls updateValueFactory with the current
   * value to compute the new value. If the state does not exist, stores the initial value.
   * Handles state marked for removal by replacing it with an UPDATE.
   *
   * This supports patterns like counters or complex updates where the new value depends
   * on the current value: `addOrUpdateState("count", 0, (name, old) => old + 1)`
   *
   * @param stateName - The name of the state.
   * @param value - The initial value if the state does not exist.
   * @param updateValueFactory - Callback that computes new value given [name, oldValue].
   * @returns The final value after the operation.
   */
  async addOrUpdateState(stateName: string, value: T, updateValueFactory: (a: string, b: T) => T): Promise<T> {
    const stateChangeTracker = this.getContextualStateTracker();

    if (stateChangeTracker.has(stateName)) {
      const stateMetadata = stateChangeTracker.get(stateName);

      if (!stateMetadata) {
        throw new Error("State Metadata was not set");
      }

      if (stateMetadata?.getChangeKind() === StateChangeKind.REMOVE) {
        stateChangeTracker.set(stateName, new StateMetadata(value, StateChangeKind.UPDATE));
        return value;
      }

      const newValue = updateValueFactory(stateName, stateMetadata.getValue());
      stateMetadata.setValue(newValue);

      if (stateMetadata.getChangeKind() === StateChangeKind.NONE) {
        stateMetadata.setChangeKind(StateChangeKind.UPDATE);
      }

      stateChangeTracker.set(stateName, stateMetadata);

      return newValue;
    }

    const [hasValue, val] = await this.actor
      .getStateProvider()
      .tryLoadState(this.actor.getActorType(), this.actor.getActorId(), stateName);

    if (hasValue) {
      const newValue = updateValueFactory(stateName, val);
      stateChangeTracker.set(stateName, new StateMetadata(newValue, StateChangeKind.UPDATE));
      return newValue;
    }

    stateChangeTracker.set(stateName, new StateMetadata(value, StateChangeKind.ADD));

    return value;
  }

  /**
   * Retrieves all state names that have been added or removed in this activation.
   *
   * Returns state names with pending changes (ADD or REMOVE), not including unchanged
   * states or states marked as NONE. Useful for audit logging or debugging.
   *
   * @returns Array of state names with pending changes.
   */
  async getStateNames(): Promise<string[]> {
    const stateChangeTracker = this.getContextualStateTracker();

    const stateNames: string[] = [];

    stateChangeTracker.forEach((val: StateMetadata<T>, key: string) => {
      if (val.getChangeKind() === StateChangeKind.ADD || val.getChangeKind() === StateChangeKind.REMOVE) {
        stateNames.push(key);
      }
    });

    return stateNames;
  }

  /**
   * Clears all cached state and pending changes.
   *
   * Typically called on actor activation to start with a clean cache. This discards
   * any uncommitted changes.
   *
   * @internal
   */
  async clearCache(): Promise<void> {
    const stateChangeTracker = this.getContextualStateTracker();
    stateChangeTracker.clear();
  }

  /**
   * Persists all pending state changes transactionally and clears tracking.
   *
   * Collects all state mutations into ActorStateChange objects, sends them to the state
   * provider for atomic persistence, and clears the cache of removed states and updates
   * tracking for the next invocation.
   *
   * This is typically called automatically after each method invocation. Changes with
   * kind NONE (unchanged) are skipped. After persistence, the change kind is reset to
   * NONE for the next invocation cycle, and removed states are purged from the cache.
   *
   * **Critical**: This is where mutations become durable. No data persists before this call.
   *
   * @throws May throw if the underlying state provider fails to persist.
   *
   * @internal
   */
  async saveState(): Promise<void> {
    const stateChangeTracker = this.getContextualStateTracker();

    if (stateChangeTracker.size === 0) {
      return;
    }

    const stateChanges: ActorStateChange<T>[] = [];
    const statesToRemove: string[] = [];

    stateChangeTracker.forEach((stateMetadata: StateMetadata<T>, stateName: string) => {
      if (stateMetadata.getChangeKind() === StateChangeKind.NONE) {
        return;
      }

      stateChanges.push(new ActorStateChange(stateName, stateMetadata.getValue(), stateMetadata.getChangeKind()));

      if (stateMetadata.getChangeKind() === StateChangeKind.REMOVE) {
        statesToRemove.push(stateName);
      }

      // Mark the state as unmodified so that tracking for next invocation is done correctly
      stateMetadata.setChangeKind(StateChangeKind.NONE);
    });

    if (stateChanges.length > 0) {
      await this.actor.getStateProvider().saveState(this.actor.getActorType(), this.actor.getActorId(), stateChanges);
    }

    for (const stateName of statesToRemove) {
      stateChangeTracker.delete(stateName);
    }
  }
}
