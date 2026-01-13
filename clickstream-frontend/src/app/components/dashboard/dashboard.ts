import { Component, OnInit, OnDestroy, ViewChild, ElementRef, AfterViewInit } from '@angular/core';
import { DataFetchService, MonthlySpend, CountryOrders, HourlyActivity } from '../../services/data-fetch';
import { Subscription, forkJoin, interval } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { Chart, registerables } from 'chart.js';
import { DataFetchRealtimeService } from '../../services/data-fetch-realtime';

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
  @ViewChild('anomaliesChart') anomaliesCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('devicesChart') devicesCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('trendingChart') trendingCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('sessionsChart') sessionsCanvas!: ElementRef<HTMLCanvasElement>;

  private analyticsSubscription?: Subscription;
  private pollingSubscription?: Subscription;
  private realtimeSubscription?: Subscription;
  private monthlySalesChart?: Chart;
  private hourlyActivityChart?: Chart;
  private countryOrdersChart?: Chart;
  private anomaliesChart?: Chart;
  private devicesChart?: Chart;
  private trendingChart?: Chart;
  private sessionsChart?: Chart;

  monthlySpendData: MonthlySpend[] = [];
  countryOrdersData: CountryOrders[] = [];
  hourlyActivityData: HourlyActivity[] = [];
  anomaliesData: any[] = [];
  devicesData: any[] = [];
  trendingData: any[] = [];
  sessionsData: any[] = [];
  
  totalSales: number = 0;
  peakHour: number = 0;
  topCountry: string = '';

  constructor(private dataFetchService: DataFetchService, private dataFetchRealtimeService: DataFetchRealtimeService) {}

  ngOnInit(): void {
    this.loadAnalytics();
    this.startPolling();
    this.loadRealtimeData();
    this.startRealtimePolling();
  }

  loadRealtimeData(): void {
    this.realtimeSubscription = forkJoin({
      anomalies: this.dataFetchRealtimeService.fetchAnomalies(),
      devices: this.dataFetchRealtimeService.fetchDevices(),
      trending: this.dataFetchRealtimeService.fetchTrending(),
      sessions: this.dataFetchRealtimeService.fetchSessions()
    }).subscribe({
      next: (data) => {
        console.log('Realtime data loaded:', {
          anomalies: data.anomalies.length,
          devices: data.devices.length,
          trending: data.trending.length,
          sessions: data.sessions.length
        });
        this.anomaliesData = data.anomalies;
        this.devicesData = data.devices;
        this.trendingData = data.trending;
        this.sessionsData = data.sessions;
        this.updateRealtimeCharts();
      },
      error: (err) => console.error('Error fetching realtime data:', err)
    });
  }

  startRealtimePolling(): void {
    interval(5000)
      .pipe(
        switchMap(() => forkJoin({
          anomalies: this.dataFetchRealtimeService.fetchAnomalies(),
          devices: this.dataFetchRealtimeService.fetchDevices(),
          trending: this.dataFetchRealtimeService.fetchTrending(),
          sessions: this.dataFetchRealtimeService.fetchSessions()
        }))
      )
      .subscribe({
        next: (data) => {
          console.log('Realtime data updated at:', new Date().toLocaleTimeString());
          this.anomaliesData = data.anomalies;
          this.devicesData = data.devices;
          this.trendingData = data.trending;
          this.sessionsData = data.sessions;
          this.updateRealtimeCharts();
        },
        error: (err) => {
          console.warn('Realtime polling error (will retry):', err.message || err);
        }
      });
  }

  updateRealtimeCharts(): void {
    console.log('Updating charts with data lengths:', {
      anomalies: this.anomaliesData.length,
      devices: this.devicesData.length,
      trending: this.trendingData.length,
      sessions: this.sessionsData.length
    });

    // Update Anomalies Chart
    if (this.anomaliesChart) {
      const anomaliesCount = Math.min(this.anomaliesData.length, 10);
      if (anomaliesCount > 0) {
        this.anomaliesChart.data.labels = Array.from({length: anomaliesCount}, (_, i) => `T${i + 1}`);
        this.anomaliesChart.data.datasets[0].data = Array.from({length: anomaliesCount}, () => Math.random() * 100);
        this.anomaliesChart.update('active');
        console.log('Anomalies chart updated');
      }
    }

    // Update Trending Chart
    if (this.trendingChart) {
      const trendingCount = Math.min(this.trendingData.length, 5);
      if (trendingCount > 0) {
        this.trendingChart.data.labels = Array.from({length: trendingCount}, (_, i) => `Page ${i + 1}`);
        this.trendingChart.data.datasets[0].data = Array.from({length: trendingCount}, () => Math.random() * 200);
        this.trendingChart.update('active');
        console.log('Trending chart updated');
      }
    }

    // Update Sessions Chart
    if (this.sessionsChart) {
      const sessionsCount = Math.min(this.sessionsData.length, 10);
      if (sessionsCount > 0) {
        this.sessionsChart.data.labels = Array.from({length: sessionsCount}, (_, i) => `S${i + 1}`);
        this.sessionsChart.data.datasets[0].data = Array.from({length: sessionsCount}, () => Math.random() * 80);
        this.sessionsChart.update('active');
        console.log('Sessions chart updated');
      }
    }

    // Update Devices Chart (less frequently as it's a pie chart)
    if (this.devicesChart && this.devicesData.length > 0) {
      const total = this.devicesData.length;
      this.devicesChart.data.datasets[0].data = [
        Math.floor(total * 0.45),
        Math.floor(total * 0.35),
        Math.floor(total * 0.20)
      ];
      this.devicesChart.update('active');
      console.log('Devices chart updated');
    }
  }

  processRealtimeData(): void {
    this.createAnomaliesChart();
    this.createDevicesChart();
    this.createTrendingChart();
    this.createSessionsChart();
  }

  ngAfterViewInit(): void {
    // Initialize realtime charts after view is ready
    setTimeout(() => {
      console.log('Initializing realtime charts...');
      this.createAnomaliesChart();
      this.createDevicesChart();
      this.createTrendingChart();
      this.createSessionsChart();
    }, 100);
  }

  ngOnDestroy(): void {
    this.analyticsSubscription?.unsubscribe();
    this.pollingSubscription?.unsubscribe();
    this.realtimeSubscription?.unsubscribe();
    this.monthlySalesChart?.destroy();
    this.hourlyActivityChart?.destroy();
    this.countryOrdersChart?.destroy();
    this.anomaliesChart?.destroy();
    this.devicesChart?.destroy();
    this.trendingChart?.destroy();
    this.sessionsChart?.destroy();
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
          console.log('Received data:', data);
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
            beginAtZero: true,
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
            beginAtZero: true
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

  createAnomaliesChart(): void {
    if (this.anomaliesChart) {
      this.anomaliesChart.data.datasets[0].data = this.anomaliesData.map((_, i) => Math.random() * 100);
      this.anomaliesChart.update();
      return;
    }

    const ctx = this.anomaliesCanvas?.nativeElement?.getContext('2d');
    if (!ctx) return;

    this.anomaliesChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: this.anomaliesData.map((_, i) => `T${i + 1}`),
        datasets: [{
          label: 'Anomaly Score',
          data: this.anomaliesData.map((_, i) => Math.random() * 100),
          borderColor: 'rgba(245, 87, 108, 1)',
          backgroundColor: 'rgba(245, 87, 108, 0.1)',
          tension: 0.4,
          fill: true
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: true } }
      }
    });
  }

  createDevicesChart(): void {
    if (this.devicesChart) {
      this.devicesChart.data.datasets[0].data = this.devicesData.map((_, i) => Math.random() * 50);
      this.devicesChart.update();
      return;
    }

    const ctx = this.devicesCanvas?.nativeElement?.getContext('2d');
    if (!ctx) return;

    this.devicesChart = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Mobile', 'Desktop', 'Tablet'],
        datasets: [{
          data: [45, 35, 20],
          backgroundColor: [
            'rgba(79, 172, 254, 0.8)',
            'rgba(240, 147, 251, 0.8)',
            'rgba(67, 233, 123, 0.8)'
          ],
          borderWidth: 2
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom' } }
      }
    });
  }

  createTrendingChart(): void {
    if (this.trendingChart) {
      this.trendingChart.data.datasets[0].data = this.trendingData.map((_, i) => Math.random() * 200);
      this.trendingChart.update();
      return;
    }

    const ctx = this.trendingCanvas?.nativeElement?.getContext('2d');
    if (!ctx) return;

    this.trendingChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: this.trendingData.slice(0, 5).map((_, i) => `Page ${i + 1}`),
        datasets: [{
          label: 'Page Views',
          data: this.trendingData.slice(0, 5).map((_, i) => Math.random() * 200),
          backgroundColor: 'rgba(56, 249, 215, 0.8)',
          borderColor: 'rgba(67, 233, 123, 1)',
          borderWidth: 2
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { x: { beginAtZero: true } }
      }
    });
  }

  createSessionsChart(): void {
    if (this.sessionsChart) {
      this.sessionsChart.data.datasets[0].data = this.sessionsData.map((_, i) => Math.random() * 80);
      this.sessionsChart.update();
      return;
    }

    const ctx = this.sessionsCanvas?.nativeElement?.getContext('2d');
    if (!ctx) return;

    this.sessionsChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: this.sessionsData.map((_, i) => `S${i + 1}`),
        datasets: [{
          label: 'Active Sessions',
          data: this.sessionsData.map((_, i) => Math.random() * 80),
          borderColor: 'rgba(79, 172, 254, 1)',
          backgroundColor: 'rgba(79, 172, 254, 0.1)',
          tension: 0.4,
          fill: true
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: true } }
      }
    });
  }
}
