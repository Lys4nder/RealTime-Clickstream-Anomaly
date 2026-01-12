import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, throwError, timer } from 'rxjs';
import { catchError, retryWhen, mergeMap } from 'rxjs/operators';
import { environment } from '../../environments/environment';

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

    fetchMonthlySpend(): Observable<MonthlySpend[]> {
        return this.http.get<MonthlySpend[]>(`${this.apiUrl}/monthly-sales`).pipe(
            this.retryWithBackoff(),
            catchError((error) => this.handleError(error, 'monthly sales data'))
        );
    }

    fetchCountryOrders(): Observable<CountryOrders[]> {
        return this.http.get<CountryOrders[]>(`${this.apiUrl}/country-stats`).pipe(
            this.retryWithBackoff(),
            catchError((error) => this.handleError(error, 'country statistics'))
        );
    }

    fetchHourlyActivity(): Observable<HourlyActivity[]> {
        return this.http.get<HourlyActivity[]>(`${this.apiUrl}/hourly-peaks`).pipe(
            this.retryWithBackoff(),
            catchError((error) => this.handleError(error, 'hourly activity data'))
        );
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
