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

@Injectable({
  providedIn: 'root',
})
export class DataFetchRealtimeService {
    private apiUrl = environment.apiUrl;
    private maxAttempts = 3;
    private retryDelay = 1000; // 1 second

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
    connectAnomaliesWebSocket(callback: (data: any) => void): WebSocket {
        const wsUrl = this.getWebSocketUrl('/realtime/ws/anomalies');
        return this.connectWebSocket(wsUrl, callback);
    }

    connectDevicesWebSocket(callback: (data: any) => void): WebSocket {
        const wsUrl = this.getWebSocketUrl('/realtime/ws/devices');
        return this.connectWebSocket(wsUrl, callback);
    }

    connectTrendingWebSocket(callback: (data: any) => void): WebSocket {
        const wsUrl = this.getWebSocketUrl('/realtime/ws/trending');
        return this.connectWebSocket(wsUrl, callback);
    }

    connectSessionsWebSocket(callback: (data: any) => void): WebSocket {
        const wsUrl = this.getWebSocketUrl('/realtime/ws/sessions');
        return this.connectWebSocket(wsUrl, callback);
    }

    private getWebSocketUrl(path: string): string {
        // Convert http/https URL to ws/wss
        const wsProtocol = this.apiUrl.startsWith('https') ? 'wss' : 'ws';
        const baseUrl = this.apiUrl.replace(/^https?:\/\//, '');
        return `${wsProtocol}://${baseUrl}${path}`;
    }

    private connectWebSocket(url: string, callback: (data: any) => void): WebSocket {
        const ws = new WebSocket(url);

        ws.onopen = () => {
            console.log(`WebSocket connected to ${url}`);
        };

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                callback(data);
            } catch (error) {
                console.error('Error parsing WebSocket message:', error);
            }
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        ws.onclose = () => {
            console.log(`WebSocket disconnected from ${url}`);
        };

        return ws;
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
