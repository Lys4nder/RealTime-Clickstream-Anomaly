import { Component, OnInit, OnDestroy, ViewChild, ElementRef, AfterViewInit } from '@angular/core';
import { DataFetchService, MonthlySpend, CountryOrders, HourlyActivity } from '../../services/data-fetch';
import { Subscription, forkJoin } from 'rxjs';
import { Chart, ChartConfiguration, registerables } from 'chart.js';

Chart.register(...registerables);

@Component({
  selector: 'app-dashboard',
  standalone: false,
  templateUrl: './dashboard.html',
  styleUrl: './dashboard.css',
})
export class Dashboard implements OnInit, OnDestroy, AfterViewInit {
  @ViewChild('monthlySalesChart') monthlySalesCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('hourlyActivityChart') hourlyActivityCanvas!: ElementRef<HTMLCanvasElement>;

  private analyticsSubscription?: Subscription;
  private monthlySalesChart?: Chart;
  private hourlyActivityChart?: Chart;
  
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

  ngAfterViewInit(): void {
    // Charts will be created after data is loaded
  }

  ngOnDestroy(): void {
    this.analyticsSubscription?.unsubscribe();
    this.monthlySalesChart?.destroy();
    this.hourlyActivityChart?.destroy();
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

    // Create charts after data is loaded
    this.createMonthlySalesChart();
    this.createHourlyActivityChart();
  }

  createMonthlySalesChart(): void {
    if (this.monthlySalesChart) {
      this.monthlySalesChart.destroy();
    }

    const ctx = this.monthlySalesCanvas.nativeElement.getContext('2d');
    if (!ctx) return;

    this.monthlySalesChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: this.monthlySpendData.map(item => this.getMonthName(item.month)),
        datasets: [{
          label: 'Sales',
          data: this.monthlySpendData.map(item => item.total_sales),
          backgroundColor: 'rgba(240, 147, 251, 0.8)',
          borderColor: 'rgba(245, 87, 108, 1)',
          borderWidth: 2
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => this.formatCurrency(context.parsed.y || 0)
            }
          }
        },
        scales: {
          y: {
            beginAtZero: false,
            ticks: {
              callback: (value) => this.formatCurrency(Number(value))
            }
          }
        }
      }
    });
  }

  createHourlyActivityChart(): void {
    if (this.hourlyActivityChart) {
      this.hourlyActivityChart.destroy();
    }

    const ctx = this.hourlyActivityCanvas.nativeElement.getContext('2d');
    if (!ctx) return;

    this.hourlyActivityChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: this.hourlyActivityData.map(item => item.hour + 'h'),
        datasets: [{
          label: 'Orders',
          data: this.hourlyActivityData.map(item => item.count),
          backgroundColor: 'rgba(79, 172, 254, 0.8)',
          borderColor: 'rgba(0, 242, 254, 1)',
          borderWidth: 2
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false }
        },
        scales: {
          y: {
            beginAtZero: false
          }
        }
      }
    });
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
