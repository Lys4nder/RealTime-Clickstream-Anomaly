import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, throwError, timer } from 'rxjs';
import { catchError, retryWhen, mergeMap } from 'rxjs/operators';
import { environment } from '../../environments/environment';

export interface Anomaly {
  [key: string]: any;
}

export interface Device {
  [key: string]: any;
}

export interface TrendingPage {
  [key: string]: any;
}

export interface Session {
  [key: string]: any;
}

/**
 * DataFetchRealtimeService provides real-time data streaming via WebSockets
 * with built-in buffering and deduplication.
 *
 * Features:
 * - Fetches historical data on connect and deduplicates it
 * - Periodic flush of WebSocket messages (configurable interval)
 * - Deduplicates messages based on unique identifiers (event_id, session_id, etc.)
 * - TTL-based seen management to prevent unbounded memory growth
 * - Reduces chart update frequency and prevents duplicate data processing
 * - Automatically cleans up buffers on WebSocket disconnect
 */
@Injectable({
  providedIn: 'root',
})
export class DataFetchRealtimeService {
  private apiUrl = environment.apiUrl;
  private maxAttempts = 3;
  private retryDelay = 1000; // 1 second

  // Configuration
  private flushIntervalMs = 1000; // Periodic flush every 1 second
  private seenTtlMs = 60000; // Keep seen keys for 60 seconds

  // Per-connection state
  private buffers: Map<string, any[]> = new Map();
  private flushTimers: Map<string, any> = new Map();
  private seenPerConnection: Map<string, Map<string, number>> = new Map(); // bufferId -> (key -> timestamp)
  private readyState: Map<string, boolean> = new Map(); // bufferId -> is history loaded

  constructor(private http: HttpClient) {}

  private retryWithBackoff<T>() {
    return retryWhen<T>((errors) =>
      errors.pipe(
        mergeMap((error, index) => {
          if (index < this.maxAttempts - 1 && this.shouldRetry(error)) {
            const delay = this.retryDelay * Math.pow(2, index);
            return timer(delay);
          }
          return throwError(() => error);
        })
      )
    );
  }

  // Real-time HTTP endpoints
  fetchAnomalies(): Observable<Anomaly[]> {
    return this.http.get<Anomaly[]>(`${this.apiUrl}/realtime/anomalies`).pipe(
      this.retryWithBackoff(),
      catchError((error) => this.handleError(error, 'anomalies data'))
    );
  }

  fetchDevices(): Observable<Device[]> {
    return this.http.get<Device[]>(`${this.apiUrl}/realtime/devices`).pipe(
      this.retryWithBackoff(),
      catchError((error) => this.handleError(error, 'devices data'))
    );
  }

  fetchTrending(): Observable<TrendingPage[]> {
    return this.http
      .get<TrendingPage[]>(`${this.apiUrl}/realtime/trending`)
      .pipe(
        this.retryWithBackoff(),
        catchError((error) => this.handleError(error, 'trending pages data'))
      );
  }

  fetchSessions(): Observable<Session[]> {
    return this.http.get<Session[]>(`${this.apiUrl}/realtime/sessions`).pipe(
      this.retryWithBackoff(),
      catchError((error) => this.handleError(error, 'sessions data'))
    );
  }

  // WebSocket connections for real-time streaming
  connectAnomaliesWebSocket(callback: (data: any[]) => void): WebSocket {
    const wsUrl = this.getWebSocketUrl('/realtime/ws/anomalies');
    const bufferId = wsUrl;

    // Mark as not ready yet
    this.readyState.set(bufferId, false);

    // Create WebSocket immediately to return
    const ws = new WebSocket(wsUrl);

    // Setup WebSocket handlers (will buffer messages until ready)
    this.setupWebSocketHandlers(ws, bufferId, callback);

    // Fetch historical data first, then enable WebSocket processing
    this.fetchAnomalies().subscribe({
      next: (history) => {
        console.log(`Fetched ${history.length} historical anomalies`);
        // Populate seen set and get deduped history
        const deduped = this.populateSeenFromHistory(bufferId, history);
        console.log(`Emitting ${deduped.length} unique historical anomalies`);
        callback(deduped);
        // Now ready to process WebSocket messages
        this.readyState.set(bufferId, true);
      },
      error: (err) => {
        console.error(
          'Failed to fetch historical anomalies, will process live data only:',
          err
        );
        // Initialize empty seen set
        this.seenPerConnection.set(bufferId, new Map());
        // Still mark as ready
        this.readyState.set(bufferId, true);
      },
    });

    return ws;
  }

  connectDevicesWebSocket(callback: (data: any[]) => void): WebSocket {
    const wsUrl = this.getWebSocketUrl('/realtime/ws/devices');
    const bufferId = wsUrl;

    this.readyState.set(bufferId, false);
    const ws = new WebSocket(wsUrl);
    this.setupWebSocketHandlers(ws, bufferId, callback);

    this.fetchDevices().subscribe({
      next: (history) => {
        console.log(`Fetched ${history.length} historical devices`);
        const deduped = this.populateSeenFromHistory(bufferId, history);
        console.log(`Emitting ${deduped.length} unique historical devices`);
        callback(deduped);
        this.readyState.set(bufferId, true);
      },
      error: (err) => {
        console.error(
          'Failed to fetch historical devices, will process live data only:',
          err
        );
        this.seenPerConnection.set(bufferId, new Map());
        this.readyState.set(bufferId, true);
      },
    });

    return ws;
  }

  connectTrendingWebSocket(callback: (data: any[]) => void): WebSocket {
    const wsUrl = this.getWebSocketUrl('/realtime/ws/trending');
    const bufferId = wsUrl;

    this.readyState.set(bufferId, false);
    const ws = new WebSocket(wsUrl);
    this.setupWebSocketHandlers(ws, bufferId, callback);

    this.fetchTrending().subscribe({
      next: (history) => {
        console.log(`Fetched ${history.length} historical trending pages`);
        const deduped = this.populateSeenFromHistory(bufferId, history);
        console.log(
          `Emitting ${deduped.length} unique historical trending pages`
        );
        callback(deduped);
        this.readyState.set(bufferId, true);
      },
      error: (err) => {
        console.error(
          'Failed to fetch historical trending pages, will process live data only:',
          err
        );
        this.seenPerConnection.set(bufferId, new Map());
        this.readyState.set(bufferId, true);
      },
    });

    return ws;
  }

  connectSessionsWebSocket(callback: (data: any[]) => void): WebSocket {
    const wsUrl = this.getWebSocketUrl('/realtime/ws/sessions');
    const bufferId = wsUrl;

    this.readyState.set(bufferId, false);
    const ws = new WebSocket(wsUrl);
    this.setupWebSocketHandlers(ws, bufferId, callback);

    this.fetchSessions().subscribe({
      next: (history) => {
        console.log(`Fetched ${history.length} historical sessions`);
        const deduped = this.populateSeenFromHistory(bufferId, history);
        console.log(`Emitting ${deduped.length} unique historical sessions`);
        callback(deduped);
        this.readyState.set(bufferId, true);
      },
      error: (err) => {
        console.error(
          'Failed to fetch historical sessions, will process live data only:',
          err
        );
        this.seenPerConnection.set(bufferId, new Map());
        this.readyState.set(bufferId, true);
      },
    });

    return ws;
  }

  // Allow configuring flush interval and seen TTL
  setFlushInterval(milliseconds: number): void {
    this.flushIntervalMs = milliseconds;
  }

  setSeenTtl(milliseconds: number): void {
    this.seenTtlMs = milliseconds;
  }

  /**
   * Generates a deterministic key for an item.
   * Priority: event_id > id > session_id+timestamp > user_id+timestamp > fallback to canonical JSON
   *
   * Note: If server provides unique event IDs in the future, those will be preferred.
   */
  private getItemKey(item: any): string {
    // Prefer event_id or id if present
    if (item.event_id) {
      return `event:${item.event_id}`;
    }
    if (item.id) {
      return `id:${item.id}`;
    }

    // Try common identifier combinations
    if (item.session_id && item.event_timestamp) {
      return `session:${item.session_id}-${item.event_timestamp}`;
    }
    if (item.user_id && item.event_timestamp) {
      return `user:${item.user_id}-${item.event_timestamp}`;
    }
    if (item.page_section && item.event_timestamp) {
      return `page:${item.page_section}-${item.event_timestamp}`;
    }
    if (item.device_type && item.event_timestamp) {
      return `device:${item.device_type}-${item.event_timestamp}`;
    }

    // Fallback: canonical JSON (sorted keys)
    return `json:${this.canonicalJson(item)}`;
  }

  /**
   * Converts an object to a canonical JSON string with sorted keys
   * for stable serialization.
   */
  private canonicalJson(obj: any): string {
    if (obj === null || obj === undefined) {
      return JSON.stringify(obj);
    }
    if (typeof obj !== 'object') {
      return JSON.stringify(obj);
    }
    if (Array.isArray(obj)) {
      return '[' + obj.map((item) => this.canonicalJson(item)).join(',') + ']';
    }

    const sortedKeys = Object.keys(obj).sort();
    const pairs = sortedKeys.map((key) => {
      return JSON.stringify(key) + ':' + this.canonicalJson(obj[key]);
    });
    return '{' + pairs.join(',') + '}';
  }

  /**
   * Populates the seen set from historical data and returns deduplicated array
   */
  private populateSeenFromHistory(bufferId: string, history: any[]): any[] {
    const seen = new Map<string, number>();
    const deduplicated: any[] = [];
    const now = Date.now();

    for (const item of history) {
      const key = this.getItemKey(item);
      if (!seen.has(key)) {
        seen.set(key, now);
        deduplicated.push(item);
      }
    }

    this.seenPerConnection.set(bufferId, seen);
    console.log(`Populated seen set for ${bufferId} with ${seen.size} keys`);
    return deduplicated;
  }

  /**
   * Starts a periodic flush timer for the given buffer
   */
  private startPeriodicFlush(
    bufferId: string,
    callback: (data: any[]) => void
  ): void {
    // Clear any existing timer
    this.stopPeriodicFlush(bufferId);

    const timer = setInterval(() => {
      this.flushBuffer(bufferId, callback);
      this.purgeExpiredSeenEntries(bufferId);
    }, this.flushIntervalMs);

    this.flushTimers.set(bufferId, timer);
    console.log(
      `Started periodic flush for ${bufferId} every ${this.flushIntervalMs}ms`
    );
  }

  /**
   * Stops the periodic flush timer
   */
  private stopPeriodicFlush(bufferId: string): void {
    const timer = this.flushTimers.get(bufferId);
    if (timer) {
      clearInterval(timer);
      this.flushTimers.delete(bufferId);
    }
  }

  /**
   * Purges expired entries from the seen set based on TTL
   */
  private purgeExpiredSeenEntries(bufferId: string): void {
    const seen = this.seenPerConnection.get(bufferId);
    if (!seen) return;

    const now = Date.now();
    const threshold = now - this.seenTtlMs;
    let purgedCount = 0;

    for (const [key, timestamp] of seen.entries()) {
      if (timestamp < threshold) {
        seen.delete(key);
        purgedCount++;
      }
    }

    if (purgedCount > 0) {
      console.log(
        `Purged ${purgedCount} expired keys from ${bufferId}, ${seen.size} remaining`
      );
    }
  }

  private getWebSocketUrl(path: string): string {
    // Convert http/https URL to ws/wss
    const wsProtocol = this.apiUrl.startsWith('https') ? 'wss' : 'ws';
    const baseUrl = this.apiUrl.replace(/^https?:\/\//, '');
    return `${wsProtocol}://${baseUrl}${path}`;
  }

  /**
   * Sets up WebSocket event handlers for a connection
   */
  private setupWebSocketHandlers(
    ws: WebSocket,
    bufferId: string,
    callback: (data: any[]) => void
  ): void {
    // Initialize buffer and seen if not exists
    if (!this.buffers.has(bufferId)) {
      this.buffers.set(bufferId, []);
    }
    if (!this.seenPerConnection.has(bufferId)) {
      this.seenPerConnection.set(bufferId, new Map());
    }

    ws.onopen = () => {
      console.log(
        `WebSocket connected to ${bufferId} - live streaming enabled`
      );
      // Start periodic flush
      this.startPeriodicFlush(bufferId, callback);
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        // Only process messages if history is loaded
        if (this.readyState.get(bufferId)) {
          this.addToBuffer(bufferId, data);
        } else {
          console.log(
            `Buffering message for ${bufferId} (waiting for history to load)`
          );
        }
      } catch (error) {
        console.error('Error parsing WebSocket message:', error);
      }
    };

    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    ws.onclose = () => {
      console.log(
        `WebSocket disconnected from ${bufferId} - live streaming stopped`
      );
      // Clean up
      this.stopPeriodicFlush(bufferId);
      this.clearBuffer(bufferId);
    };
  }

  /**
   * Adds data to buffer WITHOUT deduplication (deduplication only happens on initial history load)
   */
  private addToBuffer(bufferId: string, data: any): void {
    const buffer = this.buffers.get(bufferId) || [];
    buffer.push(data);
    this.buffers.set(bufferId, buffer);
  }

  /**
   * Flushes the buffer periodically and emits all items (no deduplication for live data)
   */
  private flushBuffer(bufferId: string, callback: (data: any[]) => void): void {
    const buffer = this.buffers.get(bufferId) || [];

    if (buffer.length === 0) {
      return;
    }

    console.log(`Periodic flush: ${buffer.length} new messages`);

    // Send all buffered items without deduplication
    callback(buffer);

    // Clear the buffer
    this.buffers.set(bufferId, []);
  }

  /**
   * Deduplicates data using the seen set from the connection
   */
  private deduplicateData(dataArray: any[], bufferId: string): any[] {
    const seen = this.seenPerConnection.get(bufferId);
    if (!seen) {
      console.warn(`No seen set for ${bufferId}, using local deduplication`);
      // Fallback to local deduplication
      const localSeen = new Set<string>();
      const deduplicated: any[] = [];

      for (const item of dataArray) {
        const key = this.getItemKey(item);
        if (!localSeen.has(key)) {
          localSeen.add(key);
          deduplicated.push(item);
        }
      }
      return deduplicated;
    }

    const deduplicated: any[] = [];
    const now = Date.now();

    for (const item of dataArray) {
      const key = this.getItemKey(item);

      if (!seen.has(key)) {
        seen.set(key, now);
        deduplicated.push(item);
      }
    }

    return deduplicated;
  }

  private clearBuffer(bufferId: string): void {
    this.stopPeriodicFlush(bufferId);
    this.buffers.delete(bufferId);
    this.seenPerConnection.delete(bufferId);
    this.readyState.delete(bufferId);
  }

  private shouldRetry(error: any): boolean {
    if (error instanceof HttpErrorResponse) {
      return error.status === 0 || (error.status >= 500 && error.status < 600);
    }
    return false;
  }

  private handleError(
    error: HttpErrorResponse,
    dataType: string = 'data'
  ): Observable<never> {
    let errorMessage = `An error occurred while fetching ${dataType}`;

    if (error.error instanceof ErrorEvent) {
      errorMessage = `Network error: ${error.error.message}`;
      console.error('Client-side error:', error.error.message);
    } else {
      // Backend returned an unsuccessful response code
      switch (error.status) {
        case 0:
          errorMessage =
            'Unable to connect to the server. Please check your network connection.';
          break;
        case 400:
          errorMessage =
            'Bad request. The server could not understand the request.';
          break;
        case 401:
          errorMessage = 'Unauthorized. Please check your credentials.';
          break;
        case 403:
          errorMessage =
            'Access forbidden. You do not have permission to access this resource.';
          break;
        case 404:
          errorMessage =
            'API endpoint not found. Please verify the server is running.';
          break;
        case 408:
          errorMessage =
            'Request timeout. The server took too long to respond.';
          break;
        case 429:
          errorMessage = 'Too many requests. Please wait before trying again.';
          break;
        case 500:
          errorMessage = 'Internal server error. Please try again later.';
          break;
        case 502:
          errorMessage = 'Bad gateway. The server is temporarily unavailable.';
          break;
        case 503:
          errorMessage = 'Service unavailable. The server is temporarily down.';
          break;
        case 504:
          errorMessage = 'Gateway timeout. The server did not respond in time.';
          break;
        default:
          errorMessage = `Server error (${error.status}): ${error.message}`;
      }
      console.error(
        `Backend error: Status ${error.status}, Message: ${error.message}`
      );
    }

    console.error('Error details:', errorMessage);
    return throwError(() => new Error(errorMessage));
  }
}
