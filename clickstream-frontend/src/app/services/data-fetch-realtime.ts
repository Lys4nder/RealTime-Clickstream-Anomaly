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
 * - Buffers incoming WebSocket messages for 500ms (configurable)
 * - Deduplicates messages based on unique identifiers (session_id, user_id, etc.)
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
    private bufferTime = 200; // Buffer messages for 200ms (reduced for faster updates)
    private buffers: Map<string, any[]> = new Map();
    private bufferTimers: Map<string, any> = new Map();

    constructor(private http: HttpClient) { }

    private retryWithBackoff<T>() {
        return retryWhen<T>(errors =>
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
        return this.http.get<TrendingPage[]>(`${this.apiUrl}/realtime/trending`).pipe(
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
        return this.connectWebSocket(wsUrl, callback);
    }

    connectDevicesWebSocket(callback: (data: any[]) => void): WebSocket {
        const wsUrl = this.getWebSocketUrl('/realtime/ws/devices');
        return this.connectWebSocket(wsUrl, callback);
    }

    connectTrendingWebSocket(callback: (data: any[]) => void): WebSocket {
        const wsUrl = this.getWebSocketUrl('/realtime/ws/trending');
        return this.connectWebSocket(wsUrl, callback);
    }

    connectSessionsWebSocket(callback: (data: any[]) => void): WebSocket {
        const wsUrl = this.getWebSocketUrl('/realtime/ws/sessions');
        return this.connectWebSocket(wsUrl, callback);
    }

    // Allow configuring buffer time
    setBufferTime(milliseconds: number): void {
        this.bufferTime = milliseconds;
    }

    private getWebSocketUrl(path: string): string {
        // Convert http/https URL to ws/wss
        const wsProtocol = this.apiUrl.startsWith('https') ? 'wss' : 'ws';
        const baseUrl = this.apiUrl.replace(/^https?:\/\//, '');
        return `${wsProtocol}://${baseUrl}${path}`;
    }

    private connectWebSocket(url: string, callback: (data: any[]) => void): WebSocket {
        const ws = new WebSocket(url);
        const bufferId = url;

        // Initialize buffer for this WebSocket
        this.buffers.set(bufferId, []);

        ws.onopen = () => {
            console.log(`WebSocket connected to ${url} - data streaming will begin shortly`);
        };

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.addToBuffer(bufferId, data, callback);
            } catch (error) {
                console.error('Error parsing WebSocket message:', error);
            }
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        ws.onclose = () => {
            console.log(`WebSocket disconnected from ${url} - data streaming has stopped`);
            // Clean up buffer and timer
            this.clearBuffer(bufferId);
        };

        return ws;
    }

    private addToBuffer(bufferId: string, data: any, callback: (data: any[]) => void): void {
        const buffer = this.buffers.get(bufferId) || [];
        buffer.push(data);
        this.buffers.set(bufferId, buffer);

        // Clear existing timer
        if (this.bufferTimers.has(bufferId)) {
            clearTimeout(this.bufferTimers.get(bufferId));
        }

        // Set new timer to flush buffer
        const timer = setTimeout(() => {
            this.flushBuffer(bufferId, callback);
        }, this.bufferTime);

        this.bufferTimers.set(bufferId, timer);
    }

    private flushBuffer(bufferId: string, callback: (data: any[]) => void): void {
        const buffer = this.buffers.get(bufferId) || [];
        
        if (buffer.length === 0) {
            return;
        }

        // Deduplicate messages based on unique identifiers
        const deduplicatedData = this.deduplicateData(buffer);
        
        console.log(`Flushed buffer: ${buffer.length} messages, ${deduplicatedData.length} unique after deduplication`);
        
        // Send entire batch to the callback
        callback(deduplicatedData);

        // Clear the buffer
        this.buffers.set(bufferId, []);
        this.bufferTimers.delete(bufferId);
    }

    private deduplicateData(dataArray: any[]): any[] {
        // Try to deduplicate based on common identifier fields
        const seen = new Set<string>();
        const deduplicated: any[] = [];

        for (const item of dataArray) {
            let key: string;
            
            if (item.session_id && item.event_timestamp) {
                key = `${item.session_id}-${item.event_timestamp}`;
            } else if (item.user_id && item.event_timestamp) {
                key = `${item.user_id}-${item.event_timestamp}`;
            } else if (item.page_section && item.event_timestamp) {
                key = `${item.page_section}-${item.event_timestamp}`;
            } else if (item.device_type && item.event_timestamp) {
                key = `${item.device_type}-${item.event_timestamp}`;
            } else {
                key = JSON.stringify(item);
            }

            if (!seen.has(key)) {
                seen.add(key);
                deduplicated.push(item);
            }
        }

        return deduplicated;
    }

    private clearBuffer(bufferId: string): void {
        if (this.bufferTimers.has(bufferId)) {
            clearTimeout(this.bufferTimers.get(bufferId));
            this.bufferTimers.delete(bufferId);
        }
        this.buffers.delete(bufferId);
    }

    private shouldRetry(error: any): boolean {
        if (error instanceof HttpErrorResponse) {
            return error.status === 0 || (error.status >= 500 && error.status < 600);
        }
        return false;
    }

    private handleError(error: HttpErrorResponse, dataType: string = 'data'): Observable<never> {
        let errorMessage = `An error occurred while fetching ${dataType}`;

        if (error.error instanceof ErrorEvent) {
            errorMessage = `Network error: ${error.error.message}`;
            console.error('Client-side error:', error.error.message);
        } else {
            // Backend returned an unsuccessful response code
            switch (error.status) {
                case 0:
                    errorMessage = 'Unable to connect to the server. Please check your network connection.';
                    break;
                case 400:
                    errorMessage = 'Bad request. The server could not understand the request.';
                    break;
                case 401:
                    errorMessage = 'Unauthorized. Please check your credentials.';
                    break;
                case 403:
                    errorMessage = 'Access forbidden. You do not have permission to access this resource.';
                    break;
                case 404:
                    errorMessage = 'API endpoint not found. Please verify the server is running.';
                    break;
                case 408:
                    errorMessage = 'Request timeout. The server took too long to respond.';
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
            console.error(`Backend error: Status ${error.status}, Message: ${error.message}`);
        }

        console.error('Error details:', errorMessage);
        return throwError(() => new Error(errorMessage));
    }
}
