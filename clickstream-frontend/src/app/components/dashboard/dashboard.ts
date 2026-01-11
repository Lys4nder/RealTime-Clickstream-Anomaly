import { Component, OnInit, OnDestroy } from '@angular/core';
import { DataFetchService, ClickEvent } from '../../services/data-fetch';
import { interval, Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';

@Component({
  selector: 'app-dashboard',
  standalone: false,
  templateUrl: './dashboard.html',
  styleUrl: './dashboard.css',
})
export class Dashboard implements OnInit, OnDestroy {
  clickEvents: ClickEvent[] = [];
  private subscription?: Subscription;
  private initialSubscription?: Subscription;
  
  // Expose Object to template
  Object = Object;
  
  totalEvents: number = 0;
  suspiciousCount: number = 0;
  normalCount: number = 0;
  
  categoryStats: { [key: string]: number } = {};
  actionTypeStats: { [key: string]: number } = {};
  deviceTypeStats: { [key: string]: number } = {};
  countryStats: { [key: string]: number } = {};

  constructor(private dataFetchService: DataFetchService) {}

  ngOnInit(): void {
    this.loadData();
  }

  ngOnDestroy(): void {
    this.subscription?.unsubscribe();
    this.initialSubscription?.unsubscribe();
  }

  loadData(): void {
    this.initialSubscription = this.dataFetchService.fetchClickEvents().subscribe({
      next: (events) => {
        this.processEvents(events);
        this.startPolling();
      },
      error: (err) => console.error('Error fetching events:', err)
    });
  }

  startPolling(): void {
    // Only start polling if not already started
    if (!this.subscription) {
      this.subscription = interval(5000)
        .pipe(switchMap(() => this.dataFetchService.fetchClickEvents()))
        .subscribe({
          next: (events) => this.processEvents(events),
          error: (err) => console.error('Error fetching events:', err)
        });
    }
  }

  processEvents(events: ClickEvent[]): void {
    this.clickEvents = events.sort((a, b) => 
      new Date(b.event_timestamp).getTime() - new Date(a.event_timestamp).getTime()
    );
    
    this.totalEvents = events.length;
    this.suspiciousCount = events.filter(e => e.isSuspicious).length;
    this.normalCount = this.totalEvents - this.suspiciousCount;
    
    this.categoryStats = {};
    this.actionTypeStats = {};
    this.deviceTypeStats = {};
    this.countryStats = {};
    
    events.forEach(event => {
      this.categoryStats[event.page_category] = (this.categoryStats[event.page_category] || 0) + 1;
      this.actionTypeStats[event.action_type] = (this.actionTypeStats[event.action_type] || 0) + 1;
      this.deviceTypeStats[event.device_type] = (this.deviceTypeStats[event.device_type] || 0) + 1;
      this.countryStats[event.country] = (this.countryStats[event.country] || 0) + 1;
    });
  }

  getPercentage(value: number): number {
    return this.totalEvents > 0 ? (value / this.totalEvents) * 100 : 0;
  }

  getObjectEntries(obj: { [key: string]: number }): Array<[string, number]> {
    return Object.entries(obj).sort((a, b) => b[1] - a[1]);
  }

  formatTimestamp(timestamp: string): string {
    return new Date(timestamp).toLocaleString();
  }
}
