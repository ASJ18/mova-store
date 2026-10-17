import { rpc, xdr, StrKey } from "@stellar/stellar-sdk";

import {
  CHECKOUT_CONTRACT_ID,
  EVENT_POLL_INTERVAL_MS,
  EVENT_START_LEDGER_BACKFILL,
  RPC_URL,
} from "./config";
import { scValToString } from "./scval";

// ----------------------------------------------------------------------------
// Real-time event indexer.
//
// Polls `getEvents` (cursor-paginated) for the checkout contract and decodes
// the contract's events (`pay`, `create_order`, `dispatch`, `refund`) so the
// UI can update instantly when a payment lands. Uses a ledger backfill on
// first connect, then advances by cursor so nothing is missed between polls.
// ----------------------------------------------------------------------------

export interface IndexedEvent {
  id: string;
  ledger: number;
  ledgerClosedAt: string;
  txHash: string;
  /** Deployed checkout contract id. */
  contractId?: string;
  /** Event name, e.g. "pay", "create_order", "dispatch", "refund". */
  symbol: string;
  /** Raw topics (including the symbol as topics[0]). */
  topics: xdr.ScVal[];
  /** Decoded fields: topics[1..] as topic1..topicN plus data-map entries. */
  fields: Record<string, string>;
}

export interface IndexerStatus {
  running: boolean;
  /** True while the indexer is started but polling is suspended (tab hidden). */
  paused: boolean;
  latestLedger?: number;
  lastCursor?: string;
  eventsSeen: number;
  lastError?: string;
  retrying?: boolean;
}

export interface IndexerCallbacks {
  onEvent: (event: IndexedEvent) => void;
  onStatus?: (status: IndexerStatus) => void;
  onError?: (error: Error) => void;
}

const RETENTION_RETRY_LEDGER_DELTA = 5;

/**
 * Whether an RPC error means the requested position fell outside the retained
 * event window (as opposed to a transient network/transport failure).
 */
function isRetentionError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error ?? "");
  return /retention|too old|older than|ledger range|out of range/i.test(message);
}

export class PaymentEventIndexer {
  private readonly server: rpc.Server;
  private readonly contractId: string;
  private readonly pollMs: number;
  private readonly watchedSymbols: string[];
  /** Durable start ledger for history-sensitive views; `undefined` keeps the rolling backfill. */
  private readonly durableStartLedger: number | undefined;
  /** When set, the resume cursor is persisted here so a reload continues the scan. */
  private readonly cursorStorageKey: string | undefined;

  private cursor: string | undefined;
  private startLedger: number | undefined;
  private latestLedger: number | undefined;
  private eventsSeen = 0;
  private lastError: string | undefined;
  private readonly seenIds = new Set<string>();

  private timer: ReturnType<typeof setInterval> | null = null;
  /** Prevents a new poll from starting while the previous one is still running. */
  private inFlight = false;
  /** Set while the document is hidden; polling resumes on visibilitychange. */
  private paused = false;
  private visibilityListener: (() => void) | null = null;
  private running = false;
  private started = false;
  private initializing = false;
  private initialized = false;

  constructor(
    opts: {
      rpcUrl?: string;
      contractId?: string;
      pollMs?: number;
      watchedSymbols?: string[];
      startLedger?: number;
      cursorStorageKey?: string;
    } = {}
  ) {
    this.server = new rpc.Server(opts.rpcUrl ?? RPC_URL);
    this.contractId = opts.contractId ?? CHECKOUT_CONTRACT_ID;
    if (!this.contractId || !StrKey.isValidContract(this.contractId)) {
      throw new Error(`Invalid or missing checkout contract ID: "${this.contractId || ""}"`);
    }
    this.pollMs = opts.pollMs ?? EVENT_POLL_INTERVAL_MS;
    this.watchedSymbols = opts.watchedSymbols ?? ["pay", "create_order", "dispatch", "refund"];
    this.durableStartLedger =
      opts.startLedger !== undefined && opts.startLedger > 0 ? opts.startLedger : undefined;
    this.cursorStorageKey = opts.cursorStorageKey;
  }

  get status(): IndexerStatus {
    return {
      running: this.running,
      paused: this.paused,
      latestLedger: this.latestLedger,
      lastCursor: this.cursor,
      eventsSeen: this.eventsSeen,
      lastError: this.lastError,
      retrying: this.running && !this.initialized,
    };
  }

  /** Begin polling. Idempotent; safe to call again after `stop`. */
  start(callbacks: IndexerCallbacks): void {
    if (this.running) return;
    this.running = true;
    this.started = true;

    this.attachVisibilityListener(callbacks);

    // A tab hat is already hidden must not schedule polls at all; the
    // visibilitychange handler resumes (with a catch-up tick) on focus.
    if (typeof document !== "undefined" && document.hidden) {
      this.paused = true;
      callbacks.onStatus?.(this.status);
      return;
    }

    this.timer = setInterval(() => void this.tick(callbacks), this.pollMs);
    void this.tick(callbacks);
  }

  stop(): void {
    this.running = false;
    this.paused = false;
    this.detachVisibilityListener();
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Re-poll immediately without tearing down state: the cursor, the seen-id set
   * and every event already delivered are kept, so a manual refresh can only
   * add to what a view has loaded, never drop it. Used by "Refresh" buttons in
   * place of a document reload, which would restart the indexer and re-scan
   * only the backfill window.
   *
   * Safe to call at any time: it is a no-op when the indexer is not running or
   * is paused (a hidden tab resumes with its own catch-up tick), and `tick`
   * itself skips the call if a poll is already in flight.
   */
  refresh(callbacks: IndexerCallbacks): void {
    if (!this.running || this.paused) return;
    void this.tick(callbacks);
  }

  private async tick(callbacks: IndexerCallbacks): Promise<void> {
    if (!this.running || this.paused) return;

    // Never let the interval outrun its own work. If the previous poll is still
    // unresolved (a slow getLatestLedger/getEvents), skip this tick entirely —
    // overlapping polls would read the same cursor, advance it out of order and
    // deliver the same event twice.
    if (this.inFlight) return;
    this.inFlight = true;

    try {
      if (!this.initialized) {
        await this.ensureInitialized(callbacks);
        if (!this.initialized) {
          return;
        }
      }

      await this.poll(callbacks);
    } finally {
      this.inFlight = false;
    }
  }

  /**
   * Background tabs should not poll: stop the interval while the document is
   * hidden so a buyer who switches tabs mid-payment stops hammering the RPC.
   */
  private attachVisibilityListener(callbacks: IndexerCallbacks): void {
    if (typeof document === "undefined" || this.visibilityListener) return;
    this.visibilityListener = () => this.handleVisibilityChange(callbacks);
    document.addEventListener("visibilitychange", this.visibilityListener);
  }

  private detachVisibilityListener(): void {
    if (this.visibilityListener && typeof document !== "undefined") {
      document.removeEventListener("visibilitychange", this.visibilityListener);
    }
    this.visibilityListener = null;
  }

  private handleVisibilityChange(callbacks: IndexerCallbacks): void {
    if (!this.running) return;
    if (typeof document !== "undefined" && document.hidden) {
      this.pause(callbacks);
    } else {
      this.resume(callbacks);
    }
  }

  private pause(callbacks: IndexerCallbacks): void {
    if (this.paused) return;
    this.paused = true;
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
    callbacks.onStatus?.(this.status);
  }

  /**
   * Resume polling and run one catch-up tick immediately. The cursor is kept
   * across the pause, so the catch-up reads every event that landed while the
   * tab was hidden — nothing is skipped.
   */
  private resume(callbacks: IndexerCallbacks): void {
    if (!this.paused) return;
    this.paused = false;
    if (this.timer === null) {
      this.timer = setInterval(() => void this.tick(callbacks), this.pollMs);
    }
    callbacks.onStatus?.(this.status);
    void this.tick(callbacks);
  }

  private async ensureInitialized(callbacks: IndexerCallbacks): Promise<void> {
    if (this.initializing) return;
    this.initializing = true;

    try {
      const latest = await this.server.getLatestLedger();
      this.latestLedger = latest.sequence;
      const persistedCursor = this.readPersistedCursor();
      if (persistedCursor) {
        // Resume the position persisted by a previous visit instead of
        // re-walking the backfill window from scratch.
        this.cursor = persistedCursor;
      } else {
        this.startLedger = this.resolveStartLedger();
      }
      this.initialized = true;
      this.lastError = undefined;
      callbacks.onStatus?.(this.status);
    } catch (err) {
      this.lastError = String(err instanceof Error ? err.message : err);
      callbacks.onError?.(
        new Error(`Could not reach the Stellar RPC (retrying): ${this.lastError}`)
      );
      callbacks.onStatus?.(this.status);
    } finally {
      this.initializing = false;
    }
  }

  /**
   * The ledger a fresh scan starts from. A configured durable start ledger wins
   * over the rolling backfill window, so a history-sensitive view can reach
   * orders older than `EVENT_START_LEDGER_BACKFILL`.
   */
  private resolveStartLedger(): number {
    if (this.durableStartLedger !== undefined) {
      return this.durableStartLedger;
    }
    if (this.latestLedger !== undefined) {
      return Math.max(1, this.latestLedger - EVENT_START_LEDGER_BACKFILL);
    }
    return 1;
  }

  private readPersistedCursor(): string | undefined {
    if (!this.cursorStorageKey || typeof window === "undefined") return undefined;
    try {
      return window.localStorage.getItem(this.cursorStorageKey) ?? undefined;
    } catch {
      // A disabled or full localStorage must not stop the scan.
      return undefined;
    }
  }

  private persistCursor(): void {
    if (!this.cursorStorageKey || !this.cursor || typeof window === "undefined") return;
    try {
      window.localStorage.setItem(this.cursorStorageKey, this.cursor);
    } catch {
      // Best-effort: an unwritable store just means the next load re-scans.
    }
  }

  private clearPersistedCursor(): void {
    if (!this.cursorStorageKey || typeof window === "undefined") return;
    try {
      window.localStorage.removeItem(this.cursorStorageKey);
    } catch {
      // Best-effort.
    }
  }

  private async poll(callbacks: IndexerCallbacks): Promise<void> {
    if (!this.running || !this.initialized) return;
    try {
      const res = await this.fetchEvents();
      this.latestLedger = res.latestLedger;
      this.lastError = undefined;

      // Once a cursor is available, drop the start-ledger window so the next
      // poll advances by cursor instead of re-scanning the backfill range. This
      // has to stay inside the `if`: clearing it on a cursor-less response left
      // the indexer with neither a cursor nor a start ledger, so every later
      // poll threw "no cursor or start ledger to poll from" and the scan stopped
      // advancing entirely.
      if (res.cursor) {
        this.cursor = res.cursor;
        this.persistCursor();
        this.startLedger = undefined;
      }

      for (const raw of res.events) {
        if (!raw.inSuccessfulContractCall) continue;
        if (this.seenIds.has(raw.id)) continue;
        this.seenIds.add(raw.id);

        const decoded = this.decodeEvent(raw);
        if (decoded) {
          this.eventsSeen += 1;
          try {
            callbacks.onEvent(decoded);
          } catch {
            // a throwing consumer must not break the poll loop
          }
        }
      }

      callbacks.onStatus?.(this.status);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.lastError = message;

      // Only a genuine retention error means the cursor/start-ledger position
      // is now unreachable. For a transient failure the window must be left
      // untouched so the next poll rescues the same range and no event is lost.
      if (isRetentionError(err)) {
        this.recoverFromRetentionError();
      }

      callbacks.onError?.(new Error(`Stellar event poll failed (retrying): ${message}`));
      callbacks.onStatus?.(this.status);
    }
  }

  /**
   * Recover from a retention error by restarting the scan just before the
   * retained window. Only called for genuine retention errors; a transient
   * failure leaves the window alone so no events are skipped.
   */
  private recoverFromRetentionError(): void {
    const from = this.latestLedger ?? this.startLedger ?? 1;
    this.cursor = undefined;
    this.startLedger = Math.max(1, from - RETENTION_RETRY_LEDGER_DELTA);
    this.clearPersistedCursor();
  }

  private async fetchEvents(): Promise<{
    events: rpc.Api.GetEventsResponse["events"];
    cursor: string | undefined;
    latestLedger: number;
  }> {
    const request: rpc.Server.GetEventsRequest = {
      filters: [
        {
          type: "contract",
          contractIds: [this.contractId],
        },
      ],
      limit: 100,
    };

    if (this.cursor) {
      request.cursor = this.cursor;
    } else if (this.startLedger !== undefined) {
      request.startLedger = this.startLedger;
    } else {
      throw new Error("no cursor or start ledger to poll from");
    }

    const res = await this.server.getEvents(request);
    return {
      events: res.events,
      cursor: res.cursor,
      latestLedger: res.latestLedger,
    };
  }

  private decodeEvent(raw: rpc.Api.EventResponse): IndexedEvent | undefined {
    const topics = raw.topic;
    if (!topics || topics.length === 0) return undefined;

    const symbol = scValToString(topics[0]);
    if (!symbol || !this.watchedSymbols.includes(symbol)) return undefined;

    const fields: Record<string, string> = {};
    for (let i = 1; i < topics.length; i++) {
      const value = scValToString(topics[i]);
      if (value !== undefined) {
        fields[`topic${i}`] = value;
      }
    }

    const data = raw.value;
    if (data && data.switch().name === "scvMap") {
      for (const entry of data.map()) {
        const key = scValToString(entry.key());
        const value = scValToString(entry.val());
        if (key !== undefined && value !== undefined) {
          fields[key] = value;
        }
      }
    }

    return {
      id: raw.id,
      ledger: raw.ledger,
      ledgerClosedAt: raw.ledgerClosedAt,
      txHash: raw.txHash,
      contractId: raw.contractId,
      symbol,
      topics,
      fields,
    };
  }
}
