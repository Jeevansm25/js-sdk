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

import { Temporal } from "@js-temporal/polyfill";
import DaprClient from "../../implementation/Client/DaprClient";
import { Logger } from "../../logger/Logger";
import ActorId from "../ActorId";
import ActorClient from "../client/ActorClient/ActorClient";
import ActorStateManager from "./ActorStateManager";
import StateProvider from "./StateProvider";

/**
 * Base class for all actors in the Dapr actor runtime.
 *
 * Actors are virtual, stateful entities that can be distributed across a system. This class
 * provides the foundation for implementing actor types, managing their lifecycle, state,
 * reminders, and timers.
 *
 * **Lifecycle**: Actors follow an activation/deactivation lifecycle:
 * - onActivate() is called when the actor is first activated
 * - Actor methods can be invoked (with onActorMethodPre/Post hooks)
 * - onDeactivate() is called when the actor is garbage collected
 *
 * **State Management**: Actors can store durable state via {@link getStateManager}.
 * All state modifications are local until saveStateInternal() is called (automatic
 * after each method invocation). This enables transactional semantics.
 *
 * **Reminders vs Timers**:
 * - Reminders: Persistent, survive failover. Called via receiveReminder().
 * - Timers: Ephemeral, per-activation. Called via explicit callback method.
 *
 * @remarks
 * Actors are typically not instantiated directly. The runtime creates instances
 * on demand through the ActorManager. When implementing a custom actor, extend this
 * class and override the lifecycle hooks as needed.
 *
 * @example
 * ```typescript
 * interface ICounterActor {
 *   increment(amount: number): Promise<number>;
 *   get(): Promise<number>;
 * }
 *
 * export class CounterActor extends AbstractActor implements ICounterActor {
 *   async increment(amount: number): Promise<number> {
 *     const current = await this.getStateManager().tryGetState("count");
 *     const newValue = (current[1] || 0) + amount;
 *     await this.getStateManager().setState("count", newValue);
 *     return newValue;
 *   }
 *
 *   async get(): Promise<number> {
 *     const [, value] = await this.getStateManager().tryGetState("count");
 *     return value || 0;
 *   }
 * }
 * ```
 */
export default abstract class AbstractActor {
  private readonly stateManager: ActorStateManager<any>;
  private readonly id: ActorId;
  private readonly daprClient: DaprClient;
  private readonly actorClient: ActorClient;
  private readonly daprStateProvider: StateProvider;
  private readonly actorType: any; // set at constructor level
  private readonly logger: Logger;

  /**
   * Constructs an AbstractActor instance.
   *
   * Typically called by the actor runtime, not directly by user code. Initializes
   * the actor with its unique identifier, Dapr client connection, state manager,
   * and internal hooks.
   *
   * @param daprClient - The DaprClient for state and actor operations.
   * @param id - The unique actor identifier.
   */
  constructor(daprClient: DaprClient, id: ActorId) {
    this.daprClient = daprClient;
    this.actorClient = new ActorClient(
      daprClient.options.daprHost,
      daprClient.options.daprPort,
      daprClient.options.communicationProtocol,
      daprClient.options,
    );
    this.logger = new Logger("Actors", "AbstractActor", daprClient.options.logger);
    this.id = id;

    this.stateManager = new ActorStateManager(this);
    this.daprStateProvider = new StateProvider(this.actorClient);

    // Interesting one: get the Class Type of the child
    this.actorType = this.constructor.name;
  }

  /**
   * Registers a persistent reminder for this actor.
   *
   * Reminders trigger callbacks at specified intervals and survive actor deactivation
   * and failover. They are persisted in the Dapr state store. If a reminder with the
   * same name is registered again, it updates the existing reminder configuration.
   *
   * The reminder callback will invoke the actor's receiveReminder() method with the
   * state data. For custom callback methods, override receiveReminder().
   *
   * @param reminderName - Unique reminder identifier within this actor.
   * @param dueTime - ISO 8601 Duration before first invocation.
   * @param period - Optional ISO 8601 Duration for recurrence interval.
   * @param ttl - Optional ISO 8601 Duration after which the reminder expires and is deleted.
   * @param state - Optional state data to pass to the reminder callback.
   *
   * @example
   * ```typescript
   * // Register a reminder that fires after 5 seconds, then every 10 seconds
   * await this.registerActorReminder(
   *   "cleanup",
   *   Temporal.Duration.from({ seconds: 5 }),
   *   Temporal.Duration.from({ seconds: 10 })
   * );
   * ```
   *
   * @see receiveReminder
   * @see unregisterActorReminder
   * @throws May throw if the registration fails in the Dapr runtime.
   */
  async registerActorReminder<_Type>(
    reminderName: string,
    dueTime: Temporal.Duration,
    period?: Temporal.Duration,
    ttl?: Temporal.Duration,
    state?: any,
  ) {
    await this.actorClient.actor.registerActorReminder(this.actorType, this.id, reminderName, {
      period,
      dueTime,
      ttl,
      data: state,
    });
  }

  /**
   * Unregisters a persistent reminder for this actor.
   *
   * The reminder will stop firing and be deleted from the state store.
   *
   * @param reminderName - The name of the reminder to unregister.
   * @throws May throw if the unregistration fails in the Dapr runtime.
   */
  async unregisterActorReminder(reminderName: string) {
    await this.actorClient.actor.unregisterActorReminder(this.actorType, this.id, reminderName);
  }

  /**
   * Registers an ephemeral timer for this actor.
   *
   * Timers trigger method callbacks at specified intervals but are not persisted.
   * They exist only for the current activation and are lost on deactivation or failover.
   * Useful for cleanup tasks, timeouts, and other ephemeral scheduling.
   *
   * The callback method must be an async method on the actor implementation.
   *
   * @param timerName - Unique timer identifier within this actor.
   * @param callback - Method name to invoke when the timer fires.
   * @param dueTime - ISO 8601 Duration before first invocation.
   * @param period - Optional ISO 8601 Duration for recurrence interval.
   * @param ttl - Optional ISO 8601 Duration after which the timer expires.
   * @param state - Optional state data to pass to the callback method.
   *
   * @example
   * ```typescript
   * // Register a timer that fires after 1 second, then every 5 seconds
   * await this.registerActorTimer(
   *   "gc",
   *   "onGarbageCollect",
   *   Temporal.Duration.from({ seconds: 1 }),
   *   Temporal.Duration.from({ seconds: 5 })
   * );
   * ```
   *
   * @see unregisterActorTimer
   * @throws May throw if the registration fails in the Dapr runtime.
   */
  async registerActorTimer(
    timerName: string,
    callback: string,
    dueTime: Temporal.Duration,
    period?: Temporal.Duration,
    ttl?: Temporal.Duration,
    state?: any,
  ) {
    // Register the timer in the sidecar
    return await this.actorClient.actor.registerActorTimer(this.actorType, this.id, timerName, {
      period,
      dueTime,
      ttl,
      data: state,
      callback,
    });
  }

  /**
   * Unregisters an ephemeral timer for this actor.
   *
   * The timer will stop firing and be removed from the runtime.
   *
   * @param timerName - The name of the timer to unregister.
   * @throws May throw if the unregistration fails in the Dapr runtime.
   */
  async unregisterActorTimer(timerName: string) {
    await this.actorClient.actor.unregisterActorTimer(this.actorType, this.id, timerName);
  }

  /**
   * Internal hook called when the actor is activated.
   *
   * Clears state cache, calls the onActivate hook, and persists any state changes.
   * Called by the runtime before any method invocations or reminders are processed.
   *
   * @internal
   */
  async onActivateInternal(): Promise<void> {
    await this.resetStateInternal();
    await this.onActivate();
    await this.saveStateInternal();
  }

  /**
   * Internal hook called when the actor is deactivated.
   *
   * Clears state cache, calls the onDeactivate hook, and persists any state changes.
   * Called by the runtime when the actor is being garbage collected or removed.
   *
   * @internal
   */
  async onDeactivateInternal(): Promise<void> {
    await this.resetStateInternal();
    await this.onDeactivate();
    await this.saveStateInternal();
  }

  /**
   * Internal hook called before a method invocation.
   *
   * Invokes the onActorMethodPre hook for custom pre-method logic.
   *
   * @internal
   */
  async onActorMethodPreInternal(): Promise<void> {
    await this.onActorMethodPre();
  }

  /**
   * Internal hook called after a method invocation.
   *
   * Invokes the onActorMethodPost hook and persists all accumulated state changes.
   * This is where mutations become durable.
   *
   * @internal
   */
  async onActorMethodPostInternal(): Promise<void> {
    await this.onActorMethodPost();

    // We need to save the state of the actor
    await this.saveStateInternal();
  }

  /**
   * Clears the in-memory state cache.
   *
   * Called on activation and after failed method invocations to start with a clean state.
   *
   * @internal
   */
  async resetStateInternal(): Promise<void> {
    await this.stateManager.clearCache();
  }

  /**
   * Persists all pending state changes to the Dapr state store.
   *
   * Called automatically after each method invocation and on deactivation.
   *
   * @internal
   */
  async saveStateInternal(): Promise<void> {
    await this.stateManager.saveState();
  }

  /**
   * Called when the actor is activated, before any method invocations.
   *
   * Override this method to perform initialization logic (e.g., loading initial state,
   * registering reminders/timers, warming up caches). Default implementation does nothing.
   *
   * @example
   * ```typescript
   * async onActivate(): Promise<void> {
   *   console.log(`Actor ${this.getActorId()} activated`);
   *   await this.registerActorReminder("periodic-sync", Temporal.Duration.from({ seconds: 60 }));
   * }
   * ```
   */
  async onActivate(): Promise<void> {
    return;
  }

  /**
   * Called when the actor is deactivated, before being garbage collected.
   *
   * Override this method to perform cleanup logic (e.g., closing connections, writing audit logs).
   * Default implementation does nothing. State is automatically persisted after this call.
   *
   * @example
   * ```typescript
   * async onDeactivate(): Promise<void> {
   *   console.log(`Actor ${this.getActorId()} deactivating`);
   * }
   * ```
   */
  async onDeactivate(): Promise<void> {
    return;
  }

  /**
   * Called before a method is invoked on the actor.
   *
   * Override this method to perform pre-method logic (e.g., logging, authentication,
   * reentrancy checks). Default implementation does nothing.
   */
  async onActorMethodPre(): Promise<void> {
    return;
  }

  /**
   * Called after a method is invoked on the actor.
   *
   * Override this method to perform post-method logic (e.g., logging, metrics collection).
   * Note: state changes are automatically persisted after this hook returns.
   * Default implementation does nothing.
   */
  async onActorMethodPost(): Promise<void> {
    return;
  }

  /**
   * Called when a persistent reminder fires.
   *
   * Override this method to handle reminder callbacks. The default implementation logs a warning.
   * For custom callback methods, override the specific method instead of this default handler.
   *
   * @param _data - State data from the reminder, if any.
   *
   * @example
   * ```typescript
   * async receiveReminder(data: string): Promise<void> {
   *   console.log(`Reminder fired with data: ${data}`);
   * }
   * ```
   */
  async receiveReminder(_data: string): Promise<void> {
    this.logger.warn(
      JSON.stringify({
        error: "ACTOR_METHOD_NOT_IMPLEMENTED",
        errorMsg: `A reminder was created for the actor with id: ${this.id} but the method 'receiveReminder' was not implemented`,
      }),
    );
  }

  /**
   * Retrieves the Dapr client for this actor.
   *
   * @returns The DaprClient instance.
   *
   * @internal
   */
  getDaprClient(): DaprClient {
    return this.daprClient;
  }

  /**
   * Retrieves the state provider for this actor.
   *
   * @returns The StateProvider for persistence operations.
   *
   * @internal
   */
  getStateProvider(): StateProvider {
    return this.daprStateProvider;
  }

  /**
   * Retrieves the state manager for this actor.
   *
   * @returns The ActorStateManager for state operations.
   */
  getStateManager<T>(): ActorStateManager<T> {
    return this.stateManager;
  }

  /**
   * Retrieves the unique identifier for this actor instance.
   *
   * @returns The ActorId.
   */
  getActorId(): ActorId {
    return this.id;
  }

  /**
   * Retrieves the actor type name.
   *
   * @returns The class name of the concrete actor implementation.
   */
  getActorType(): any {
    return this.actorType;
  }
}
