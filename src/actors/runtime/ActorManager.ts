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

import DaprClient from "../../implementation/Client/DaprClient";
import Class from "../../types/Class";
import ActorId from "../ActorId";
import AbstractActor from "./AbstractActor";
import ActorReminderData from "./ActorReminderData";
import ActorTimerData from "./ActorTimerData";
import BufferSerializer from "./BufferSerializer";

/**
 * Manages instances of a specific actor type.
 *
 * ActorManager handles actor lifecycle (creation, activation, deactivation), method invocation,
 * and reminder/timer dispatch for a single actor type. It maintains a cache of active actor
 * instances and routes incoming requests to the appropriate actor instance.
 *
 * @remarks
 * - Actors are lazily activated on first access
 * - Active actors are cached by ID for efficient reuse
 * - All method invocations go through pre/post hooks for state management
 * - Reminders and timers are dispatched through the standard method invocation path
 *
 * **Known Limitations** (marked as @todo in code):
 * - Race condition protection for concurrent access not yet implemented
 * - Reentrancy checks not yet implemented
 *
 * @template T - The actor type managed by this manager, must extend {@link AbstractActor}.
 *
 * @internal
 */
const REMINDER_METHOD_NAME = "receiveReminder"; // the callback method name for the reminder

export default class ActorManager<T extends AbstractActor> {
  /**
   * The actor class constructor for this manager.
   */
  readonly actorCls: Class<T>;

  /**
   * The Dapr client used for actor operations.
   */
  readonly daprClient: DaprClient;

  /**
   * Serializer for buffer/object conversions.
   *
   * @internal
   */
  readonly serializer: BufferSerializer = new BufferSerializer();

  /**
   * Map of active actor instances, keyed by actor ID string.
   */
  actors: Map<string, T>;

  // dispatcher: ActorMethodDispatcher<T>;
  // timerMethodContext: any;
  // reminderMethodContext: any;

  /**
   * Constructs an ActorManager for the given actor type.
   *
   * @param actorCls - The actor class to manage.
   * @param daprClient - The Dapr client for operations.
   */
  constructor(actorCls: Class<T>, daprClient: DaprClient) {
    this.daprClient = daprClient;
    this.actorCls = actorCls;

    this.actors = new Map<string, T>();

    // @todo: we need to make sure race condition cannot happen when accessing the active actors
    // NodeJS has an event loop (main thread -> runs JS code) and a worker pool (threadpool -> automatically created for offloading work through libuv) threads
    // we can have a new thread through the worker_thread module
    // https://medium.com/@mohllal/node-js-multithreading-a5cd74958a67
    //
    //
    // Python: asyncio.lock -> implements a mutex lock for asyncio tasks to guarantee exclusive access to a shared resource
    // Java: Collections.synchronizedMap -> is a thread-saf synchronized map to guarantee serial access
    // NodeJS: https://nodejs.org/api/worker_threads.html
    // this.activeActorsLock = null; // Unknown in JS, states: asyncio.lock() in python @todo: we need a Mutex locking function

    // this.dispatcher = new ActorMethodDispatcher(this.runtimeCtx.getActorTypeInformation());
    // this.timerMethodContext = ActorMethodContext.createForTimer(TIMER_METHOD_NAME);
    // this.reminderMethodContext = ActorMethodContext.createForReminder(REMINDER_METHOD_NAME);
  }

  /**
   * Creates a new actor instance without adding it to the cache.
   *
   * @param actorId - The actor identifier.
   * @returns A new actor instance.
   *
   * @internal
   */
  async createActor(actorId: ActorId): Promise<T> {
    return new this.actorCls(this.daprClient, actorId);
  }

  /**
   * Activates an actor, calling its onActivate lifecycle hook.
   *
   * Creates the actor instance, calls onActivateInternal(), and caches it by ID.
   *
   * @param actorId - The actor identifier.
   *
   * @internal
   */
  async activateActor(actorId: ActorId): Promise<void> {
    const actor = await this.createActor(actorId);

    // We activate the actor by calling the onActivateInternal method on it
    // this will create its state object
    await actor.onActivateInternal();

    this.actors.set(actorId.getId(), actor);
  }

  /**
   * Deactivates an actor, calling its onDeactivate lifecycle hook and removing from cache.
   *
   * @param actorId - The actor identifier.
   * @throws Error if the actor is not currently active.
   *
   * @internal
   */
  async deactivateActor(actorId: ActorId): Promise<void> {
    if (!this.actors.has(actorId.getId())) {
      throw new Error(
        JSON.stringify({
          error: "ACTOR_NOT_ACTIVATED",
          errorMsg: `The actor ${actorId.getId()} was not activated`,
        }),
      );
    }

    const actor = await this.getActiveActor(actorId);
    await actor.onDeactivateInternal();

    this.actors.delete(actorId.getId());
  }

  /**
   * Gets an active actor instance, activating if needed (lazy activation).
   *
   * If the actor is not in the cache, activates it first.
   *
   * @param actorId - The actor identifier.
   * @returns The active actor instance.
   * @throws Error if activation fails.
   *
   * @internal
   */
  async getActiveActor(actorId: ActorId): Promise<T> {
    if (!this.actors.has(actorId.getId())) {
      await this.activateActor(actorId);
    }

    const actor = this.actors.get(actorId.getId());

    if (!actor) {
      throw new Error(`${actorId.getId()} was not activated correctly`);
    }

    return actor;
  }

  /**
   * Invokes a method on an actor with deserialized parameters.
   *
   * Deserializes the request buffer, calls the method via callActorMethod, and returns
   * the result. Automatically handles pre/post method hooks for state management.
   *
   * @param actorId - The actor identifier.
   * @param actorMethodName - The method name to invoke.
   * @param requestBody - Optional serialized request data.
   * @returns The method result.
   * @throws Error if the method does not exist or invocation fails.
   *
   * @internal
   */
  async invoke(actorId: ActorId, actorMethodName: string, requestBody?: Buffer): Promise<any> {
    const requestBodyDeserialized = this.serializer.deserialize(requestBody || Buffer.from(""));
    return await this.callActorMethod(actorId, actorMethodName, requestBodyDeserialized);
  }

  /**
   * Fires a persistent reminder on an actor.
   *
   * Deserializes the reminder data, reconstructs the ActorReminderData, and invokes
   * the receiveReminder method with the reminder state.
   *
   * @param actorId - The actor identifier.
   * @param reminderName - The reminder name.
   * @param requestBody - Serialized reminder data.
   *
   * @internal
   */
  async fireReminder(actorId: ActorId, reminderName: string, requestBody?: Buffer): Promise<void> {
    // @todo: make sure we are remindable
    const requestBodyDeserialized = this.serializer.deserialize(requestBody || Buffer.from(""));
    const reminderData = ActorReminderData.fromObject(reminderName, requestBodyDeserialized as object);
    await this.callActorMethod(actorId, REMINDER_METHOD_NAME, reminderData.state);
  }

  /**
   * Fires an ephemeral timer on an actor.
   *
   * Deserializes the timer data, reconstructs the ActorTimerData, and invokes
   * the callback method with the timer state.
   *
   * @param actorId - The actor identifier.
   * @param timerName - The timer name.
   * @param requestBody - Serialized timer data.
   *
   * @internal
   */
  async fireTimer(actorId: ActorId, timerName: string, requestBody?: Buffer): Promise<void> {
    // @todo: make sure we are remindable
    const requestBodyDeserialized = this.serializer.deserialize(requestBody || Buffer.from(""));
    const timerData = ActorTimerData.fromObject(timerName, requestBodyDeserialized as object);
    await this.callActorMethod(actorId, timerData.callback, timerData.state);
  }

  /**
   * Calls an actor method with arguments, handling lifecycle hooks and state management.
   *
   * Validates the method exists, invokes pre-hook, calls the method (spreading array args),
   * invokes post-hook (which persists state), and returns the result.
   *
   * @param actorId - The actor identifier.
   * @param actorMethodName - The method name to invoke.
   * @param args - Method arguments (can be a single value or array for spreading).
   * @returns The method result as a Buffer.
   * @throws Error if the method does not exist or invocation fails.
   *
   * @internal
   */
  async callActorMethod(actorId: ActorId, actorMethodName: string, args: any): Promise<Buffer> {
    const actorObject = await this.getActiveActor(actorId);

    // Check if the actor method exists? Skip type-checking as it's the power of Javascript
    // @ts-ignore
    if (typeof actorObject[actorMethodName] !== "function") {
      throw new Error(
        JSON.stringify({
          error: "ACTOR_METHOD_DOES_NOT_EXIST",
          errorMsg: `The actor method '${actorMethodName}' does not exist on ${this.actorCls.name}`,
        }),
      );
    }

    // @todo: actor reentrancy

    // Call the actor method, Skip type-checking as it's the power of Javascript
    await actorObject.onActorMethodPreInternal();

    let res;

    // If we have an array, we passed multiple parameters and should thus spread those
    if (Array.isArray(args)) {
      // @ts-ignore
      res = await actorObject[actorMethodName](...args);
    } else {
      // @ts-ignore
      res = await actorObject[actorMethodName](args);
    }

    // Invoke post-hook (which persists state changes made during the method)
    await actorObject.onActorMethodPostInternal();

    // Return the result serialized as a Buffer
    return this.serializer.serialize(res);
  }
}
