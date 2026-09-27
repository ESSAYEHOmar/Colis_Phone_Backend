export type LocalEntryStatus = "pending" | "synced";

export interface ProcessResult {
  success: boolean;
  message: string;
}

export interface OfflineScanEntry {
  id: string;
  payloadStr: string;
  weight: number;
  tariff: number;
  createdAt: string;
}

export interface LocalSavedEntry extends OfflineScanEntry {
  status: LocalEntryStatus;
  syncedAt?: string;
}

export interface OfflineQueueStorage {
  getItem(key: string): Promise<string | null>;
  setItem(key: string, value: string): Promise<void>;
}

export interface ShipmentMutationClient {
  processScannedShipment(
    payloadStr: string,
    weight: number,
    tariff: number
  ): Promise<ProcessResult>;
}
