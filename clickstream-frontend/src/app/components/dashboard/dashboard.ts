import { Component, OnInit, OnDestroy, ViewChild, ElementRef, AfterViewInit } from '@angular/core';
import { DataFetchService, MonthlySpend, CountryOrders, HourlyActivity } from '../../services/data-fetch';
import { Subscription, forkJoin, interval } from 'rxjs';
import { switchMap } from 'rxjs/operators';
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
  @ViewChild('countryOrdersChart') countryOrdersCanvas!: ElementRef<HTMLCanvasElement>;

  private analyticsSubscription?: Subscription;
  private pollingSubscription?: Subscription;
  private monthlySalesChart?: Chart;
  private hourlyActivityChart?: Chart;
  private countryOrdersChart?: Chart;
  
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
    this.startPolling();
  }

  ngAfterViewInit(): void {
    // Charts will be created after data is loaded
  }

  ngOnDestroy(): void {
    this.analyticsSubscription?.unsubscribe();
    this.pollingSubscription?.unsubscribe();
    this.monthlySalesChart?.destroy();
    this.hourlyActivityChart?.destroy();
    this.countryOrdersChart?.destroy();
  }

  startPolling(): void {
    // Poll every 5 minutes
    this.pollingSubscription = interval(1000 * 60 * 5)
      .pipe(
        switchMap(() => forkJoin({
          monthlySpend: this.dataFetchService.fetchMonthlySpend(),
          countryOrders: this.dataFetchService.fetchCountryOrders(),
          hourlyActivity: this.dataFetchService.fetchHourlyActivity()
        }))
      )
      .subscribe({
        next: (data) => {
          console.log('Data refreshed at:', new Date().toLocaleTimeString());
          this.monthlySpendData = data.monthlySpend;
          this.countryOrdersData = data.countryOrders;
          this.hourlyActivityData = data.hourlyActivity;
          this.processAnalytics();
        },
        error: (err) => console.error('Error polling analytics:', err)
      });
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
    this.createCountryOrdersChart();
    this.createHourlyActivityChart();
  }

  createMonthlySalesChart(): void {
    if (this.monthlySalesChart) {
      this.monthlySalesChart.data.labels = this.monthlySpendData.map(item => this.getMonthName(item.month));
      this.monthlySalesChart.data.datasets[0].data = this.monthlySpendData.map(item => item.total_sales);
      this.monthlySalesChart.update();
      return;
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
      this.hourlyActivityChart.data.labels = this.hourlyActivityData.map(item => item.hour + 'h');
      this.hourlyActivityChart.data.datasets[0].data = this.hourlyActivityData.map(item => item.count);
      this.hourlyActivityChart.update();
      return;
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
createCountryOrdersChart(): void {
    if (this.countryOrdersChart) {
      this.countryOrdersChart.data.labels = this.countryOrdersData.map(item => item.shipping_country);
      this.countryOrdersChart.data.datasets[0].data = this.countryOrdersData.map(item => item.count);
      this.countryOrdersChart.update();
      return;
    }

    const ctx = this.countryOrdersCanvas.nativeElement.getContext('2d');
    if (!ctx) return;

    this.countryOrdersChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: this.countryOrdersData.map(item => item.shipping_country),
        datasets: [{
          label: 'Orders',
          data: this.countryOrdersData.map(item => item.count),
          backgroundColor: 'rgba(67, 233, 123, 0.8)',
          borderColor: 'rgba(56, 249, 215, 1)',
          borderWidth: 2
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false }
        },
        scales: {
          x: {
            beginAtZero: true
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
