import { Component, OnInit, OnDestroy } from '@angular/core';
import { DataFetchService, ClickEvent, MonthlySpend, CountryOrders, HourlyActivity } from '../../services/data-fetch';
import { interval, Subscription, forkJoin } from 'rxjs';
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
  private analyticsSubscription?: Subscription;
  
  // Expose Object to template
  Object = Object;
  
  totalEvents: number = 0;
  suspiciousCount: number = 0;
  normalCount: number = 0;
  
  categoryStats: { [key: string]: number } = {};
  actionTypeStats: { [key: string]: number } = {};
  deviceTypeStats: { [key: string]: number } = {};
  countryStats: { [key: string]: number } = {};

  monthlySpendData: MonthlySpend[] = [];
  countryOrdersData: CountryOrders[] = [];
  hourlyActivityData: HourlyActivity[] = [];
  
  totalSales: number = 0;
  peakHour: number = 0;
  topCountry: string = '';

  constructor(private dataFetchService: DataFetchService) {}

  ngOnInit(): void {
    this.loadData();
    this.loadAnalytics();
  }

  ngOnDestroy(): void {
    this.subscription?.unsubscribe();
    this.initialSubscription?.unsubscribe();
    this.analyticsSubscription?.unsubscribe();
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
    if (!this.subscription) {
      this.subscription = interval(5000)
        .pipe(switchMap(() => this.dataFetchService.fetchClickEvents()))
        .subscribe({
          next: (events) => this.processEvents(events),
          error: (err) => console.error('Error fetching events:', err)
        });
    }
  }

  loadAnalytics(): void {
    this.analyticsSubscription = forkJoin({
      monthlySpend: this.dataFetchService.fetchMonthlySpend(),
      countryOrders: this.dataFetchService.fetchCountryOrders(),
      hourlyActivity: this.dataFetchService.fetchHourlyActivity()
    }).subscribe({
      next: (data) => {
        this.monthlySpendData = data.monthlySpend;
        this.countryOrdersData = data.countryOrders;
        this.hourlyActivityData = data.hourlyActivity;
        this.processAnalytics();
      },
      error: (err) => console.error('Error fetching analytics:', err)
    });
  }

  processAnalytics(): void {
    this.totalSales = this.monthlySpendData.reduce((sum, item) => sum + item.total_sales, 0);
    
    if (this.hourlyActivityData.length > 0) {
      const peak = this.hourlyActivityData.reduce((max, item) => 
        item.count > max.count ? item : max
      );
      this.peakHour = peak.hour;
    }
    
    if (this.countryOrdersData.length > 0) {
      this.topCountry = this.countryOrdersData[0].shipping_country;
    }
  }

  getMonthName(month: number): string {
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    return months[month - 1] || '';
  }

  formatCurrency(value: number): string {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(value);
  }

  getMaxValue(data: any[], key: string): number {
    if (data.length === 0) return 1;
    return Math.max(...data.map(item => item[key]));
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
