import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, throwError, timer } from 'rxjs';
import { catchError, retry, retryWhen, mergeMap, finalize } from 'rxjs/operators';

export interface ClickEvent {
  event_timestamp: string;
  session_id: string;
  user_id: string;
  ip_address: string;
  country: string;
  click_sequence: number;
  page_category: string;
  product_code: string;
  action_type: string;
  device_type: string;
  page_section: string;
  isSuspicious: boolean;
}

export interface MonthlySpend {
  month: number;
  total_sales: number;
}

export interface CountryOrders {
  shipping_country: string;
  count: number;
}

export interface HourlyActivity {
  hour: number;
  count: number;
}

@Injectable({
  providedIn: 'root',
})
export class DataFetchService {
    private apiUrl = 'https://api.example.http://localhost:8080/api/click-events';
    private analyticsUrl = 'http://localhost:8080/api/analytics';
    private maxRetries = 3;
    private retryDelay = 1000; // 1 second

    constructor(private http: HttpClient) { }

    fetchClickEvents(): Observable<ClickEvent[]> {
        return this.http.get<ClickEvent[]>(this.apiUrl).pipe(
            retryWhen(errors =>
                errors.pipe(
                    mergeMap((error, index) => {
                        if (index < this.maxRetries && this.shouldRetry(error)) {
                            const delay = this.retryDelay * Math.pow(2, index);
                            console.warn(`Retrying request (${index + 1}/${this.maxRetries}) after ${delay}ms...`);
                            return timer(delay);
                        }
                        return throwError(() => error);
                    })
                )
            ),
            catchError((error) => this.handleError(error))
        );
    }

    fetchMonthlySpend(): Observable<MonthlySpend[]> {
        return this.http.get<MonthlySpend[]>(`${this.analyticsUrl}/monthly-spend`).pipe(
            retryWhen(errors =>
                errors.pipe(
                    mergeMap((error, index) => {
                        if (index < this.maxRetries && this.shouldRetry(error)) {
                            const delay = this.retryDelay * Math.pow(2, index);
                            return timer(delay);
                        }
                        return throwError(() => error);
                    })
                )
            ),
            catchError((error) => this.handleError(error))
        );
    }

    fetchCountryOrders(): Observable<CountryOrders[]> {
        return this.http.get<CountryOrders[]>(`${this.analyticsUrl}/country-orders`).pipe(
            retryWhen(errors =>
                errors.pipe(
                    mergeMap((error, index) => {
                        if (index < this.maxRetries && this.shouldRetry(error)) {
                            const delay = this.retryDelay * Math.pow(2, index);
                            return timer(delay);
                        }
                        return throwError(() => error);
                    })
                )
            ),
            catchError((error) => this.handleError(error))
        );
    }

    fetchHourlyActivity(): Observable<HourlyActivity[]> {
        return this.http.get<HourlyActivity[]>(`${this.analyticsUrl}/hourly-activity`).pipe(
            retryWhen(errors =>
                errors.pipe(
                    mergeMap((error, index) => {
                        if (index < this.maxRetries && this.shouldRetry(error)) {
                            const delay = this.retryDelay * Math.pow(2, index);
                            return timer(delay);
                        }
                        return throwError(() => error);
                    })
                )
            ),
            catchError((error) => this.handleError(error))
        );
    }

    private shouldRetry(error: any): boolean {
        if (error instanceof HttpErrorResponse) {
            return error.status === 0 || (error.status >= 500 && error.status < 600);
        }
        return false;
    }

    private handleError(error: HttpErrorResponse): Observable<never> {
        let errorMessage = 'An error occurred while fetching click events';

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
