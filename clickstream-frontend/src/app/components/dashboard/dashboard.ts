import { Component, OnInit, OnDestroy } from '@angular/core';
import { DataFetchService, MonthlySpend, CountryOrders, HourlyActivity } from '../../services/data-fetch';
import { Subscription, forkJoin } from 'rxjs';

@Component({
  selector: 'app-dashboard',
  standalone: false,
  templateUrl: './dashboard.html',
  styleUrl: './dashboard.css',
})
export class Dashboard implements OnInit, OnDestroy {
  private analyticsSubscription?: Subscription;
  
  // Expose Object to template
  Object = Object;

  monthlySpendData: MonthlySpend[] = [];
  countryOrdersData: CountryOrders[] = [];
  hourlyActivityData: HourlyActivity[] = [];
  
  totalSales: number = 0;
  peakHour: number = 0;
  topCountry: string = '';

  constructor(private dataFetchService: DataFetchService) {}

  ngOnInit(): void {
    this.loadAnalytics();
  }

  ngOnDestroy(): void {
    this.analyticsSubscription?.unsubscribe();
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

  getMinValue(data: any[], key: string): number {
    if (data.length === 0) return 0;
    return Math.min(...data.map(item => item[key]));
  }

  getBarHeight(value: number, data: any[], key: string): number {
    if (data.length === 0) return 0;
    const min = this.getMinValue(data, key);
    const max = this.getMaxValue(data, key);
    const range = max - min;
    
    // If all values are the same, return full height
    if (range === 0) return 100;
    
    // Calculate percentage from min to max with 10% padding at bottom
    const percentage = ((value - min) / range) * 90 + 10;
    return percentage;
  }
}
