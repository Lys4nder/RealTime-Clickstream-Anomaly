import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/internal/Observable';

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

@Injectable({
  providedIn: 'root',
})
export class DataFetch {
    private apiUrl = 'https://api.example.http://localhost:8080/api/click-events';

    constructor(private http: HttpClient) { }

    fetchClickEvents(): Observable<ClickEvent[]> {
        return this.http.get<ClickEvent[]>(this.apiUrl);
    }
}
