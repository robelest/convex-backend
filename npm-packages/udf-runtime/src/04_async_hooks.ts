/**
 * AsyncLocalStorage implementation for V8 environments.
 *
 * This is a prototype implementation of Node.js's AsyncLocalStorage API
 * (from `node:async_hooks`) adapted for the Convex V8 runtime environment.
 *
 * Async context storage is backed by V8 continuation-preserved embedder data,
 * allowing context propagation across async/await without patching Promise
 * prototypes.
 *
 * API reference: https://nodejs.org/api/async_context.html#class-asynclocalstorage
 */

// The async context is stored as an array: [ALS1, value1, ALS2, value2, ...]
// We use undefined to represent an empty context
type AsyncContextData = ReadonlyArray<unknown> | undefined;

type ConvexAsyncContextBridge = {
  getContinuationPreservedEmbedderData?: () => unknown;
  setContinuationPreservedEmbedderData?: (value: unknown) => void;
};

// Fallback used only when running outside the Convex backend runtime.
let fallbackAsyncContext: AsyncContextData = undefined;

function getConvexAsyncContextBridge(): ConvexAsyncContextBridge | undefined {
  return (globalThis as { Convex?: ConvexAsyncContextBridge }).Convex;
}

function normalizeAsyncContext(value: unknown): AsyncContextData {
  if (value === undefined) {
    return undefined;
  }
  if (Array.isArray(value)) {
    return value;
  }
  return undefined;
}

/**
 * Get the current async context
 */
function getAsyncContext(): AsyncContextData {
  const convex = getConvexAsyncContextBridge();
  if (convex?.getContinuationPreservedEmbedderData) {
    return normalizeAsyncContext(convex.getContinuationPreservedEmbedderData());
  }
  return fallbackAsyncContext;
}

/**
 * Set the current async context
 */
function setAsyncContext(context: AsyncContextData): void {
  const convex = getConvexAsyncContextBridge();
  if (convex?.setContinuationPreservedEmbedderData) {
    convex.setContinuationPreservedEmbedderData(context);
    return;
  }
  fallbackAsyncContext = context;
}

/**
 * AsyncLocalStorage provides a way to propagate a value through async operations.
 *
 * Each instance of AsyncLocalStorage maintains its own storage that is isolated
 * from other instances. The storage is propagated through promise chains and
 * async function calls.
 *
 * @example
 * ```typescript
 * const als = new AsyncLocalStorage<{ userId: string }>();
 *
 * als.run({ userId: "123" }, async () => {
 *   console.log(als.getStore()?.userId); // "123"
 *   await someAsyncOperation();
 *   console.log(als.getStore()?.userId); // still "123"
 * });
 * ```
 */
export class AsyncLocalStorage<T = unknown> {
  #disabled = false;

  /**
   * Binds the given function to the current execution context.
   *
   * @param fn - The function to bind
   * @param args - Additional arguments to prepend when calling the bound function
   * @returns A new function that will restore the captured context when called
   */
  static bind<F extends (...args: unknown[]) => unknown>(
    fn: F,
    ...args: unknown[]
  ): (...callArgs: unknown[]) => ReturnType<F> {
    if (typeof fn !== "function") {
      throw new TypeError("fn must be a function");
    }
    const boundSnapshot = AsyncLocalStorage.snapshot();
    return (...callArgs: unknown[]) =>
      boundSnapshot(fn, ...args, ...callArgs) as ReturnType<F>;
  }

  /**
   * Captures the current execution context and returns a function that
   * runs the provided callback within that captured context.
   *
   * @returns A function that accepts a callback and runs it in the captured context
   */
  static snapshot(): <R, TArgs extends unknown[]>(
    fn: (...args: TArgs) => R,
    ...args: TArgs
  ) => R {
    const context = getAsyncContext();
    return <R, TArgs extends unknown[]>(
      fn: (...args: TArgs) => R,
      ...args: TArgs
    ): R => {
      const prev = getAsyncContext();
      setAsyncContext(context);
      try {
        return fn(...args);
      } finally {
        setAsyncContext(prev);
      }
    };
  }

  /**
   * Transitions into the context for the remainder of the current
   * synchronous execution and then persists the store through any
   * following asynchronous calls.
   *
   * @param store - The value to store in this AsyncLocalStorage
   */
  enterWith(store: T): void {
    // Re-enable if previously disabled
    this.#disabled = false;

    const context = getAsyncContext();
    if (!context) {
      setAsyncContext([this, store]);
      return;
    }

    const { length } = context;
    for (let i = 0; i < length; i += 2) {
      if (context[i] === this) {
        // Clone and update
        const clone = context.slice();
        clone[i + 1] = store;
        setAsyncContext(clone);
        return;
      }
    }

    // Not found, append
    setAsyncContext(context.concat(this, store));
  }

  /**
   * Runs a function synchronously outside of a context and returns its return value.
   *
   * @param callback - The function to run
   * @param args - Arguments to pass to the callback
   * @returns The return value of the callback
   */
  exit<R, TArgs extends unknown[]>(
    callback: (...args: TArgs) => R,
    ...args: TArgs
  ): R {
    return this.run(undefined as T, callback, ...args);
  }

  /**
   * Runs a function synchronously within a context and returns its return value.
   * The store is not accessible outside of the callback function.
   * The store is accessible to any asynchronous operations created within the callback.
   *
   * @param store - The value to store
   * @param callback - The function to run
   * @param args - Arguments to pass to the callback
   * @returns The return value of the callback
   */
  run<R, TArgs extends unknown[]>(
    store: T,
    callback: (...args: TArgs) => R,
    ...args: TArgs
  ): R {
    // Re-enable if previously disabled
    const wasDisabled = this.#disabled;
    this.#disabled = false;

    let context = getAsyncContext() as unknown[] | undefined;
    let hasPrevious = false;
    let previousValue: unknown;
    let index = -1;
    const contextWasEmpty = !context;

    if (contextWasEmpty) {
      setAsyncContext((context = [this, store]));
      index = 0;
    } else {
      // Clone before mutating
      context = context!.slice();
      index = context.indexOf(this);

      if (index > -1) {
        hasPrevious = true;
        previousValue = context[index + 1];
        context[index + 1] = store;
      } else {
        index = context.length;
        context.push(this, store);
      }
      setAsyncContext(context);
    }

    try {
      return callback(...args);
    } finally {
      if (!wasDisabled) {
        let context2 = getAsyncContext() as unknown[] | undefined;

        if (context2 === context && contextWasEmpty) {
          // Simple case: we created the context and nothing else modified it
          setAsyncContext(undefined);
        } else if (context2) {
          // Clone before mutating
          context2 = context2.slice();

          if (hasPrevious) {
            // Restore previous value
            context2[index + 1] = previousValue;
            setAsyncContext(context2);
          } else {
            // Remove our entry
            context2.splice(index, 2);
            setAsyncContext(context2.length ? context2 : undefined);
          }
        }
      }
    }
  }

  /**
   * Disables the AsyncLocalStorage instance. All subsequent calls
   * to `getStore()` will return `undefined`.
   *
   * To re-enable, call `run()` or `enterWith()`.
   */
  disable(): void {
    if (this.#disabled) return;
    this.#disabled = true;

    const context = getAsyncContext() as unknown[] | undefined;
    if (context) {
      const { length } = context;
      for (let i = 0; i < length; i += 2) {
        if (context[i] === this) {
          const newContext = context.slice();
          newContext.splice(i, 2);
          setAsyncContext(newContext.length ? newContext : undefined);
          break;
        }
      }
    }
  }

  /**
   * Returns the current store value, or `undefined` if this instance
   * is disabled or called outside of an asynchronous context.
   *
   * @returns The current store value
   */
  getStore(): T | undefined {
    if (this.#disabled) return undefined;

    const context = getAsyncContext();
    if (!context) return undefined;

    const { length } = context;
    for (let i = 0; i < length; i += 2) {
      if (context[i] === this) {
        return context[i + 1] as T;
      }
    }

    return undefined;
  }
}

/**
 * AsyncResource is a helper class that allows associating asynchronous
 * operations with their triggering context.
 *
 * This is a simplified implementation that captures and restores
 * the async context, compatible with the AsyncLocalStorage API.
 */
export class AsyncResource {
  readonly type: string;
  #snapshot: AsyncContextData;

  /**
   * Creates a new AsyncResource instance.
   *
   * @param type - The type of async resource
   * @param _options - Optional configuration (triggerAsyncId is not used in this implementation)
   */
  constructor(type: string, _options?: { triggerAsyncId?: number } | number) {
    if (typeof type !== "string") {
      throw new TypeError("type must be a string");
    }

    this.type = type;
    this.#snapshot = getAsyncContext();
  }

  /**
   * Runs a function in the async context of this resource.
   *
   * @param fn - The function to run
   * @param thisArg - The `this` value for the function
   * @param args - Arguments to pass to the function
   * @returns The return value of the function
   */
  runInAsyncScope<R, This, TArgs extends unknown[]>(
    fn: (this: This, ...args: TArgs) => R,
    thisArg?: This,
    ...args: TArgs
  ): R {
    const prev = getAsyncContext();
    setAsyncContext(this.#snapshot);
    try {
      return fn.apply(thisArg as This, args);
    } finally {
      setAsyncContext(prev);
    }
  }

  /**
   * Binds a function to run in the async context of this resource.
   *
   * @param fn - The function to bind
   * @param thisArg - The `this` value for the function (defaults to this AsyncResource)
   * @returns A bound function
   */
  bind<F extends (...args: unknown[]) => unknown>(fn: F, thisArg?: unknown): F {
    if (typeof fn !== "function") {
      throw new TypeError("fn must be a function");
    }
    // Capture the snapshot to avoid 'this' aliasing
    const snapshot = this.#snapshot;
    const bound = function (this: unknown, ...args: unknown[]) {
      const prev = getAsyncContext();
      setAsyncContext(snapshot);
      try {
        return fn.apply(thisArg ?? this, args);
      } finally {
        setAsyncContext(prev);
      }
    };
    return bound as F;
  }

  /**
   * Static helper to bind a function to the current async context.
   *
   * @param fn - The function to bind
   * @param type - The resource type (defaults to function name or "bound-anonymous-fn")
   * @param thisArg - The `this` value for the function
   * @returns A bound function
   */
  static bind<F extends (...args: unknown[]) => unknown>(
    fn: F,
    type?: string,
    thisArg?: unknown,
  ): F {
    type = type || fn.name || "bound-anonymous-fn";
    return new AsyncResource(type).bind(fn, thisArg);
  }

  // These are stubs for Node.js compatibility
  emitBefore(): boolean {
    return true;
  }

  emitAfter(): boolean {
    return true;
  }

  asyncId(): number {
    return 0;
  }

  triggerAsyncId(): number {
    return 0;
  }

  emitDestroy(): void {
    // No-op
  }
}

/**
 * Stub implementations for Node.js async_hooks compatibility.
 * These functions are deprecated in Node.js and would impact performance,
 * so they are implemented as no-ops that return placeholder values.
 */

/**
 * Returns a placeholder async ID. In the full Node.js implementation,
 * this would return the asyncId of the current execution context.
 */
export function executionAsyncId(): number {
  return 0;
}

/**
 * Returns a placeholder trigger async ID. In the full Node.js implementation,
 * this would return the ID of the resource that triggered the current context.
 */
export function triggerAsyncId(): number {
  return 0;
}

/**
 * Returns a placeholder resource. In the full Node.js implementation,
 * this would return the resource object that triggered the current context.
 */
export function executionAsyncResource(): object {
  return {};
}

/**
 * Creates a hook object (stub implementation).
 * The hooks will never be called in this implementation.
 */
export function createHook(_hooks: {
  init?: (
    asyncId: number,
    type: string,
    triggerAsyncId: number,
    resource: object,
  ) => void;
  before?: (asyncId: number) => void;
  after?: (asyncId: number) => void;
  destroy?: (asyncId: number) => void;
  promiseResolve?: (asyncId: number) => void;
}): { enable: () => object; disable: () => object } {
  return {
    enable() {
      return this;
    },
    disable() {
      return this;
    },
  };
}

/**
 * Async wrap provider types (for Node.js compatibility)
 */
export const asyncWrapProviders = {
  NONE: 0,
  DIRHANDLE: 1,
  DNSCHANNEL: 2,
  ELDHISTOGRAM: 3,
  FILEHANDLE: 4,
  FILEHANDLECLOSEREQ: 5,
  FIXEDSIZEBLOBCOPY: 6,
  FSEVENTWRAP: 7,
  FSREQCALLBACK: 8,
  FSREQPROMISE: 9,
  GETADDRINFOREQWRAP: 10,
  GETNAMEINFOREQWRAP: 11,
  HEAPSNAPSHOT: 12,
  HTTP2SESSION: 13,
  HTTP2STREAM: 14,
  HTTP2PING: 15,
  HTTP2SETTINGS: 16,
  HTTPINCOMINGMESSAGE: 17,
  HTTPCLIENTREQUEST: 18,
  JSSTREAM: 19,
  JSUDPWRAP: 20,
  MESSAGEPORT: 21,
  PIPECONNECTWRAP: 22,
  PIPESERVERWRAP: 23,
  PIPEWRAP: 24,
  PROCESSWRAP: 25,
  PROMISE: 26,
  QUERYWRAP: 27,
  SHUTDOWNWRAP: 28,
  SIGNALWRAP: 29,
  STATWATCHER: 30,
  STREAMPIPE: 31,
  TCPCONNECTWRAP: 32,
  TCPSERVERWRAP: 33,
  TCPWRAP: 34,
  TTYWRAP: 35,
  UDPSENDWRAP: 36,
  UDPWRAP: 37,
  SIGINTWATCHDOG: 38,
  WORKER: 39,
  WORKERHEAPSNAPSHOT: 40,
  WRITEWRAP: 41,
  ZLIB: 42,
  CHECKPRIMEREQUEST: 43,
  PBKDF2REQUEST: 44,
  KEYPAIRGENREQUEST: 45,
  KEYGENREQUEST: 46,
  KEYEXPORTREQUEST: 47,
  CIPHERREQUEST: 48,
  DERIVEBITSREQUEST: 49,
  HASHREQUEST: 50,
  RANDOMBYTESREQUEST: 51,
  RANDOMPRIMEREQUEST: 52,
  SCRYPTREQUEST: 53,
  SIGNREQUEST: 54,
  TLSWRAP: 55,
  VERIFYREQUEST: 56,
  INSPECTORJSBINDING: 57,
};

/**
 * Setup function to install AsyncLocalStorage and AsyncResource on the global object.
 */
export function setupAsyncHooks(global: typeof globalThis): void {
  // Create an async_hooks-like module object.
  // getAsyncContext/setAsyncContext are exposed for internal runtime wiring
  // and for compatibility with consumers expecting async_hooks internals.
  const asyncHooksModule = {
    AsyncLocalStorage,
    AsyncResource,
    createHook,
    executionAsyncId,
    triggerAsyncId,
    executionAsyncResource,
    asyncWrapProviders,
    getAsyncContext,
    setAsyncContext,
  };

  // Expose on global for direct access
  (global as Record<string, unknown>).AsyncLocalStorage = AsyncLocalStorage;
  (global as Record<string, unknown>).AsyncResource = AsyncResource;

  // Also expose a module-like object for compatibility
  (global as Record<string, unknown>).__async_hooks__ = asyncHooksModule;
}
