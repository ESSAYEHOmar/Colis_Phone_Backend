import type {
  LocalSavedEntry,
  OfflineQueueStorage,
  OfflineScanEntry,
  ProcessResult,
  ShipmentMutationClient,
} from "./types";

const QUEUE_KEY = "offline_scan_queue";
const HISTORY_KEY = "offline_scan_history";

export class OfflineScanQueue {
  private storage: OfflineQueueStorage;
  private mutationClient: ShipmentMutationClient;
  private syncInProgress = false;
  private onQueueChange: ((count: number) => void) | null = null;

  constructor(storage: OfflineQueueStorage, mutationClient: ShipmentMutationClient) {
    this.storage = storage;
    this.mutationClient = mutationClient;
  }

  setOnQueueChange(callback: (count: number) => void): void {
    this.onQueueChange = callback;
  }

  async getPendingCount(): Promise<number> {
    const queue = await this.readQueue();
    return queue.length;
  }

  async getPendingEntries(): Promise<OfflineScanEntry[]> {
    return this.readQueue();
  }

  async getLocalHistory(): Promise<LocalSavedEntry[]> {
    await this.migrateQueueIntoHistory();
    const history = await this.readHistory();
    return history.sort(
      (left, right) =>
        new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime()
    );
  }

  async enqueue(payloadStr: string, weight: number, tariff: number): Promise<void> {
    const queue = await this.readQueue();
    const entry: OfflineScanEntry = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
      payloadStr,
      weight,
      tariff,
      createdAt: new Date().toISOString(),
    };
    queue.push(entry);
    await this.writeQueue(queue);

    const history = await this.readHistory();
    history.push({ ...entry, status: "pending" });
    await this.writeHistory(history);

    await this.notifyQueueChange();
  }

  async flushQueue(): Promise<{ synced: number; failed: number }> {
    if (this.syncInProgress) {
      return { synced: 0, failed: 0 };
    }

    this.syncInProgress = true;
    let synced = 0;
    let failed = 0;

    try {
      const queue = await this.readQueue();
      const remaining: OfflineScanEntry[] = [];

      for (const entry of queue) {
        try {
          const result = await this.mutationClient.processScannedShipment(
            entry.payloadStr,
            entry.weight,
            entry.tariff
          );
          if (result.success) {
            synced += 1;
            await this.markHistorySynced(entry.id);
          } else {
            remaining.push(entry);
            failed += 1;
          }
        } catch {
          remaining.push(entry);
          failed += 1;
          break;
        }
      }

      await this.writeQueue(remaining);
      await this.notifyQueueChange();
      return { synced, failed };
    } finally {
      this.syncInProgress = false;
    }
  }

  startBackgroundSync(intervalMs: number): () => void {
    const timer = setInterval(() => {
      void this.flushQueue();
    }, intervalMs);
    return () => clearInterval(timer);
  }

  private async migrateQueueIntoHistory(): Promise<void> {
    const queue = await this.readQueue();
    const history = await this.readHistory();
    const knownIds = new Set(history.map((entry) => entry.id));
    const migrated = queue
      .filter((entry) => !knownIds.has(entry.id))
      .map((entry) => ({ ...entry, status: "pending" as const }));

    if (migrated.length === 0) {
      return;
    }

    await this.writeHistory([...history, ...migrated]);
  }

  private async markHistorySynced(entryId: string): Promise<void> {
    const history = await this.readHistory();
    const updated = history.map((entry) =>
      entry.id === entryId
        ? { ...entry, status: "synced" as const, syncedAt: new Date().toISOString() }
        : entry
    );
    await this.writeHistory(updated);
  }

  private async readQueue(): Promise<OfflineScanEntry[]> {
    return this.readArray<OfflineScanEntry>(QUEUE_KEY);
  }

  private async writeQueue(queue: OfflineScanEntry[]): Promise<void> {
    await this.storage.setItem(QUEUE_KEY, JSON.stringify(queue));
  }

  private async readHistory(): Promise<LocalSavedEntry[]> {
    return this.readArray<LocalSavedEntry>(HISTORY_KEY);
  }

  private async writeHistory(history: LocalSavedEntry[]): Promise<void> {
    await this.storage.setItem(HISTORY_KEY, JSON.stringify(history));
  }

  private async readArray<T>(key: string): Promise<T[]> {
    const raw = await this.storage.getItem(key);
    if (!raw) {
      return [];
    }
    try {
      const parsed: unknown = JSON.parse(raw);
      if (!Array.isArray(parsed)) {
        return [];
      }
      return parsed as T[];
    } catch {
      return [];
    }
  }

  private async notifyQueueChange(): Promise<void> {
    if (!this.onQueueChange) {
      return;
    }
    const count = await this.getPendingCount();
    this.onQueueChange(count);
  }
}

export function isConnectionFailure(error: unknown): boolean {
  if (error instanceof TypeError) {
    return true;
  }
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    return (
      message.includes("network") ||
      message.includes("fetch") ||
      message.includes("failed to fetch") ||
      message.includes("timeout")
    );
  }
  return false;
}
