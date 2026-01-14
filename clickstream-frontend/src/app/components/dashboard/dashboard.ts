import {
  Component,
  OnInit,
  OnDestroy,
  ViewChild,
  ElementRef,
  AfterViewInit,
} from '@angular/core';
import {
  DataFetchService,
  MonthlySpend,
  CountryOrders,
  HourlyActivity,
} from '../../services/data-fetch';
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
  @ViewChild('monthlySalesChart')
  monthlySalesCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('hourlyActivityChart')
  hourlyActivityCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('countryOrdersChart')
  countryOrdersCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('anomaliesChart') anomaliesCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('devicesChart') devicesCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('trendingChart') trendingCanvas!: ElementRef<HTMLCanvasElement>;
  @ViewChild('sessionsChart') sessionsCanvas!: ElementRef<HTMLCanvasElement>;

  private analyticsSubscription?: Subscription;
  private pollingSubscription?: Subscription;
  private realtimeSubscription?: Subscription;
  // WebSocket connections for real-time streaming
  private anomaliesWs?: WebSocket;
  private devicesWs?: WebSocket;
  private trendingWs?: WebSocket;
  private sessionsWs?: WebSocket;
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

  constructor(
    private dataFetchService: DataFetchService,
    private dataFetchRealtimeService: DataFetchRealtimeService
  ) {}

  ngOnInit(): void {
    this.loadAnalytics();
    this.startPolling();
    this.connectWebSockets();
  }

  loadRealtimeData(): void {
    // Load each dataset independently - don't wait for all to complete
    this.dataFetchRealtimeService.fetchAnomalies().subscribe({
      next: (data) => {
        console.log('Anomalies loaded:', data.length, 'items');
        console.log('Anomalies preview:', data.slice(0, 3));
        this.anomaliesData = data;
        this.updateAnomaliesChart();
      },
      error: (err) => console.error('Error fetching anomalies:', err),
    });

    this.dataFetchRealtimeService.fetchDevices().subscribe({
      next: (data) => {
        console.log('Devices loaded:', data.length, 'items');
        console.log('Devices preview:', data.slice(0, 3));
        this.devicesData = data;
        this.updateDevicesChart();
      },
      error: (err) => console.error('Error fetching devices:', err),
    });

    this.dataFetchRealtimeService.fetchTrending().subscribe({
      next: (data) => {
        console.log('Trending loaded:', data.length, 'items');
        console.log('Trending preview:', data.slice(0, 3));
        this.trendingData = data;
        this.updateTrendingChart();
      },
      error: (err) => console.error('Error fetching trending:', err),
    });

    this.dataFetchRealtimeService.fetchSessions().subscribe({
      next: (data) => {
        console.log('Sessions loaded:', data.length, 'items');
        console.log('Sessions preview:', data.slice(0, 3));
        this.sessionsData = data;
        this.updateSessionsChart();
      },
      error: (err) => console.error('Error fetching sessions:', err),
    });
  }

  connectWebSockets(): void {
    console.log('Connecting to WebSockets for real-time streaming...');

    // Anomalies WebSocket - receives batches
    this.anomaliesWs = this.dataFetchRealtimeService.connectAnomaliesWebSocket(
      (dataBatch) => {
        console.log(`Anomaly batch received: ${dataBatch.length} items`);
        this.anomaliesData = [...dataBatch, ...this.anomaliesData].slice(
          0,
          100
        );
        this.updateAnomaliesChart();
      }
    );

    // Devices WebSocket - receives batches
    this.devicesWs = this.dataFetchRealtimeService.connectDevicesWebSocket(
      (dataBatch) => {
        console.log(`Device batch received: ${dataBatch.length} items`);
        this.devicesData = [...dataBatch, ...this.devicesData].slice(0, 100);
        this.updateDevicesChart();
      }
    );

    // Trending WebSocket - receives batches
    this.trendingWs = this.dataFetchRealtimeService.connectTrendingWebSocket(
      (dataBatch) => {
        console.log(`Trending batch received: ${dataBatch.length} items`);
        this.trendingData = [...dataBatch, ...this.trendingData].slice(0, 100);
        this.updateTrendingChart();
      }
    );

    // Sessions WebSocket - receives batches
    this.sessionsWs = this.dataFetchRealtimeService.connectSessionsWebSocket(
      (dataBatch) => {
        console.log(`Session batch received: ${dataBatch.length} items`);
        this.sessionsData = [...dataBatch, ...this.sessionsData].slice(0, 100);
        this.updateSessionsChart();
      }
    );
  }

  // Individual chart update methods for WebSocket streaming
  updateAnomaliesChart(): void {
    if (this.anomaliesChart && this.anomaliesData.length > 0) {
      // Group by timestamp and aggregate
      const groupedByTime = new Map<
        string,
        { timestamp: Date; totalActions: number }
      >();

      this.anomaliesData.forEach((item) => {
        const date = new Date(item.event_timestamp);
        const timeKey = `${date.getHours()}:${date
          .getMinutes()
          .toString()
          .padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}`;

        if (groupedByTime.has(timeKey)) {
          const existing = groupedByTime.get(timeKey)!;
          existing.totalActions += item.actions_count || 0;
        } else {
          groupedByTime.set(timeKey, {
            timestamp: date,
            totalActions: item.actions_count || 0,
          });
        }
      });

      // Sort by timestamp and take most recent 10
      const sortedData = Array.from(groupedByTime.entries())
        .sort((a, b) => b[1].timestamp.getTime() - a[1].timestamp.getTime())
        .slice(0, 10)
        .reverse(); // Reverse for chart display

      const timeLabels = sortedData.map(([timeKey]) => timeKey);
      const actionsData = sortedData.map(([, data]) => data.totalActions);

      this.anomaliesChart.data.labels!.length = 0;
      this.anomaliesChart.data.labels!.push(...timeLabels);
      this.anomaliesChart.data.datasets[0].data.length = 0;
      (this.anomaliesChart.data.datasets[0].data as number[]).push(
        ...actionsData
      );
      this.anomaliesChart.update(); // Update with default animation disabled in chart config
    }
  }

  updateDevicesChart(): void {
    if (this.devicesChart && this.devicesData.length > 0) {
      const deviceTotals = this.devicesData.reduce((acc, item) => {
        const type = (item.device_type || 'UNKNOWN').toUpperCase();
        acc[type] = (acc[type] || 0) + (item.device_count || 1);
        return acc;
      }, {} as Record<string, number>);

      const deviceValues = [
        deviceTotals['MOBILE'] || 0,
        deviceTotals['DESKTOP'] || 0,
        deviceTotals['TABLET'] || 0,
      ];

      // Update values in-place instead of clearing
      deviceValues.forEach((value, i) => {
        (this.devicesChart!.data.datasets[0].data as number[])[i] = value;
      });

      this.devicesChart.update(); // Update with default animation disabled in chart config
    }
  }

  updateTrendingChart(): void {
    if (this.trendingChart && this.trendingData.length > 0) {
      console.log(
        `Updating trending chart with ${this.trendingData.length} items`
      );
      // Aggregate visit counts by page_section to eliminate duplicates
      const pageTotals = this.trendingData.reduce((acc, item) => {
        const page = item.page_section || 'Unknown';
        acc[page] = (acc[page] || 0) + (item.visit_count || 1);
        return acc;
      }, {} as Record<string, number>);

      console.log('Page totals:', pageTotals);

      // Sort by visit count and take top 10
      const topTrending = Object.entries(pageTotals)
        .sort((a, b) => (b[1] as number) - (a[1] as number))
        .slice(0, 10);

      const pageLabels = topTrending.map(([page]) => page);
      const viewCounts = topTrending.map(([, count]) => count as number);

      console.log('Setting labels:', pageLabels, 'counts:', viewCounts);

      // Always update data arrays directly (ensures no duplicates)
      this.trendingChart.data.labels = pageLabels;
      this.trendingChart.data.datasets[0].data = viewCounts as number[];

      this.trendingChart.update(); // Update with default animation disabled in chart config
    } else {
      console.log(
        'Trending chart not ready or no data:',
        this.trendingChart ? 'chart exists' : 'no chart',
        this.trendingData.length,
        'items'
      );
    }
  }

  updateSessionsChart(): void {
    if (this.sessionsChart && this.sessionsData.length > 0) {
      // Group by timestamp and aggregate
      const groupedByTime = new Map<
        string,
        { timestamp: Date; totalEvents: number; count: number }
      >();

      this.sessionsData.forEach((item) => {
        const date = new Date(item.event_timestamp);
        const timeKey = `${date.getHours()}:${date
          .getMinutes()
          .toString()
          .padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}`;

        if (groupedByTime.has(timeKey)) {
          const existing = groupedByTime.get(timeKey)!;
          existing.totalEvents += item.events_in_session || 1;
          existing.count++;
        } else {
          groupedByTime.set(timeKey, {
            timestamp: date,
            totalEvents: item.events_in_session || 1,
            count: 1,
          });
        }
      });

      // Sort by timestamp and take most recent 10
      const sortedData = Array.from(groupedByTime.entries())
        .sort((a, b) => b[1].timestamp.getTime() - a[1].timestamp.getTime())
        .slice(0, 10)
        .reverse(); // Reverse for chart display

      const sessionLabels = sortedData.map(([timeKey]) => timeKey);
      const sessionCounts = sortedData.map(([, data]) =>
        Math.round(data.totalEvents / data.count)
      ); // Average

      this.sessionsChart.data.labels!.length = 0;
      this.sessionsChart.data.labels!.push(...sessionLabels);
      this.sessionsChart.data.datasets[0].data.length = 0;
      (this.sessionsChart.data.datasets[0].data as number[]).push(
        ...sessionCounts
      );
      this.sessionsChart.update(); // Update with default animation disabled in chart config
    }
  }

  updateRealtimeCharts(): void {
    // Call individual chart update methods
    this.updateAnomaliesChart();
    this.updateTrendingChart();
    this.updateSessionsChart();
    this.updateDevicesChart();
  }

  processRealtimeData(): void {
    this.createAnomaliesChart();
    this.createDevicesChart();
    this.createTrendingChart();
    this.createSessionsChart();
  }

  ngAfterViewInit(): void {
    console.log(
      'Initializing realtime charts (empty, waiting for WebSocket data)'
    );
    this.createAnomaliesChart();
    this.createDevicesChart();
    this.createTrendingChart();
    this.createSessionsChart();
  }

  ngOnDestroy(): void {
    this.analyticsSubscription?.unsubscribe();
    this.pollingSubscription?.unsubscribe();
    this.realtimeSubscription?.unsubscribe();
    // Close WebSocket connections
    this.anomaliesWs?.close();
    this.devicesWs?.close();
    this.trendingWs?.close();
    this.sessionsWs?.close();
    // Destroy charts
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
        switchMap(() =>
          forkJoin({
            monthlySpend: this.dataFetchService.fetchMonthlySpend(),
            countryOrders: this.dataFetchService.fetchCountryOrders(),
            hourlyActivity: this.dataFetchService.fetchHourlyActivity(),
          })
        )
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
        error: (err) => console.error('Error polling analytics:', err),
      });
  }

  loadAnalytics(): void {
    this.analyticsSubscription = forkJoin({
      monthlySpend: this.dataFetchService.fetchMonthlySpend(),
      countryOrders: this.dataFetchService.fetchCountryOrders(),
      hourlyActivity: this.dataFetchService.fetchHourlyActivity(),
    }).subscribe({
      next: (data) => {
        this.monthlySpendData = data.monthlySpend;
        this.countryOrdersData = data.countryOrders;
        this.hourlyActivityData = data.hourlyActivity;
        this.processAnalytics();
      },
      error: (err) => console.error('Error fetching analytics:', err),
    });
  }

  processAnalytics(): void {
    this.totalSales = this.monthlySpendData.reduce(
      (sum, item) => sum + item.total_sales,
      0
    );

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
      this.monthlySalesChart.data.labels = this.monthlySpendData.map((item) =>
        this.getMonthName(item.month)
      );
      this.monthlySalesChart.data.datasets[0].data = this.monthlySpendData.map(
        (item) => item.total_sales
      );
      this.monthlySalesChart.update();
      return;
    }

    const ctx = this.monthlySalesCanvas.nativeElement.getContext('2d');
    if (!ctx) return;

    this.monthlySalesChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: this.monthlySpendData.map((item) =>
          this.getMonthName(item.month)
        ),
        datasets: [
          {
            label: 'Sales',
            data: this.monthlySpendData.map((item) => item.total_sales),
            backgroundColor: 'rgba(240, 147, 251, 0.8)',
            borderColor: 'rgba(245, 87, 108, 1)',
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => this.formatCurrency(context.parsed.y || 0),
            },
          },
        },
        scales: {
          y: {
            beginAtZero: true,
            ticks: {
              callback: (value) => this.formatCurrency(Number(value)),
            },
          },
        },
      },
    });
  }

  createHourlyActivityChart(): void {
    if (this.hourlyActivityChart) {
      this.hourlyActivityChart.data.labels = this.hourlyActivityData.map(
        (item) => item.hour + 'h'
      );
      this.hourlyActivityChart.data.datasets[0].data =
        this.hourlyActivityData.map((item) => item.count);
      this.hourlyActivityChart.update();
      return;
    }

    const ctx = this.hourlyActivityCanvas.nativeElement.getContext('2d');
    if (!ctx) return;

    this.hourlyActivityChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: this.hourlyActivityData.map((item) => item.hour + 'h'),
        datasets: [
          {
            label: 'Orders',
            data: this.hourlyActivityData.map((item) => item.count),
            backgroundColor: 'rgba(79, 172, 254, 0.8)',
            borderColor: 'rgba(0, 242, 254, 1)',
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
        },
        scales: {
          y: {
            beginAtZero: true,
          },
        },
      },
    });
  }
  createCountryOrdersChart(): void {
    if (this.countryOrdersChart) {
      this.countryOrdersChart.data.labels = this.countryOrdersData.map(
        (item) => item.shipping_country
      );
      this.countryOrdersChart.data.datasets[0].data =
        this.countryOrdersData.map((item) => item.count);
      this.countryOrdersChart.update();
      return;
    }

    const ctx = this.countryOrdersCanvas.nativeElement.getContext('2d');
    if (!ctx) return;

    this.countryOrdersChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: this.countryOrdersData.map((item) => item.shipping_country),
        datasets: [
          {
            label: 'Orders',
            data: this.countryOrdersData.map((item) => item.count),
            backgroundColor: 'rgba(67, 233, 123, 0.8)',
            borderColor: 'rgba(56, 249, 215, 1)',
            borderWidth: 2,
          },
        ],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
        },
        scales: {
          x: {
            beginAtZero: true,
          },
        },
      },
    });
  }

  getMonthName(month: number): string {
    const months = [
      'Jan',
      'Feb',
      'Mar',
      'Apr',
      'May',
      'Jun',
      'Jul',
      'Aug',
      'Sep',
      'Oct',
      'Nov',
      'Dec',
    ];
    return months[month - 1] || '';
  }

  formatCurrency(value: number): string {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  }

  createAnomaliesChart(): void {
    if (this.anomaliesChart) {
      this.updateRealtimeCharts();
      return;
    }

    const ctx = this.anomaliesCanvas?.nativeElement?.getContext('2d');
    if (!ctx) return;

    // Sort by timestamp and get recent data
    const recentAnomalies = [...this.anomaliesData]
      .sort(
        (a, b) =>
          new Date(b.event_timestamp).getTime() -
          new Date(a.event_timestamp).getTime()
      )
      .slice(0, 10);
    const timeLabels = recentAnomalies
      .map((item) => {
        const date = new Date(item.event_timestamp);
        return `${date.getHours()}:${date
          .getMinutes()
          .toString()
          .padStart(2, '0')}`;
      })
      .reverse();
    const actionsData = recentAnomalies
      .map((item) => item.actions_count || 0)
      .reverse();

    this.anomaliesChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: timeLabels,
        datasets: [
          {
            label: 'Actions Count',
            data: actionsData,
            borderColor: 'rgba(245, 87, 108, 1)',
            backgroundColor: 'rgba(245, 87, 108, 0.1)',
            tension: 0.4,
            fill: true,
            pointBackgroundColor: this.anomaliesData
              .slice(0, 10)
              .map((item) =>
                item.is_anomaly ? 'rgba(255, 0, 0, 1)' : 'rgba(245, 87, 108, 1)'
              )
              .reverse(),
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => `Actions: ${context.parsed.y}`,
            },
          },
        },
        scales: {
          y: { beginAtZero: true, title: { display: true, text: 'Actions' } },
        },
      },
    });
  }

  createDevicesChart(): void {
    if (this.devicesChart) {
      this.updateRealtimeCharts();
      return;
    }

    const ctx = this.devicesCanvas?.nativeElement?.getContext('2d');
    if (!ctx) return;

    // Aggregate device counts by type
    const deviceTotals = this.devicesData.reduce((acc, item) => {
      const type = (item.device_type || 'UNKNOWN').toUpperCase();
      acc[type] = (acc[type] || 0) + (item.device_count || 1);
      return acc;
    }, {} as Record<string, number>);

    this.devicesChart = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Mobile', 'Desktop', 'Tablet'],
        datasets: [
          {
            data: [
              deviceTotals['MOBILE'] || 0,
              deviceTotals['DESKTOP'] || 0,
              deviceTotals['TABLET'] || 0,
            ],
            backgroundColor: [
              'rgba(79, 172, 254, 0.8)',
              'rgba(240, 147, 251, 0.8)',
              'rgba(67, 233, 123, 0.8)',
            ],
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' },
          tooltip: {
            callbacks: {
              label: (context) => `${context.label}: ${context.parsed} events`,
            },
          },
        },
      },
    });
  }

  createTrendingChart(): void {
    if (this.trendingChart) {
      this.updateRealtimeCharts();
      return;
    }

    const ctx = this.trendingCanvas?.nativeElement?.getContext('2d');
    if (!ctx) return;

    // Get top trending pages by visit_count
    const topTrending = [...this.trendingData]
      .sort((a, b) => (b.visit_count || 0) - (a.visit_count || 0))
      .slice(0, 10);
    const pageLabels = topTrending.map(
      (item) => item.page_section || 'Unknown'
    );
    const viewCounts = topTrending.map((item) => item.visit_count || 0);

    this.trendingChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: pageLabels,
        datasets: [
          {
            label: 'Page Views',
            data: viewCounts,
            backgroundColor: 'rgba(56, 249, 215, 0.8)',
            borderColor: 'rgba(67, 233, 123, 1)',
            borderWidth: 2,
          },
        ],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => `Views: ${context.parsed.x}`,
            },
          },
        },
        scales: {
          x: { beginAtZero: true, title: { display: true, text: 'Views' } },
        },
      },
    });
  }

  createSessionsChart(): void {
    if (this.sessionsChart) {
      this.updateRealtimeCharts();
      return;
    }

    const ctx = this.sessionsCanvas?.nativeElement?.getContext('2d');
    if (!ctx) return;

    // Sort by timestamp and show session activity over time
    const recentSessions = [...this.sessionsData]
      .sort(
        (a, b) =>
          new Date(b.event_timestamp).getTime() -
          new Date(a.event_timestamp).getTime()
      )
      .slice(0, 10);
    const sessionLabels = recentSessions
      .map((item) => {
        const date = new Date(item.event_timestamp);
        return `${date.getHours()}:${date
          .getMinutes()
          .toString()
          .padStart(2, '0')}`;
      })
      .reverse();
    const sessionCounts = recentSessions
      .map((item) => item.events_in_session || 1)
      .reverse();

    this.sessionsChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: sessionLabels,
        datasets: [
          {
            label: 'Active Sessions',
            data: sessionCounts,
            borderColor: 'rgba(79, 172, 254, 1)',
            backgroundColor: 'rgba(79, 172, 254, 0.1)',
            tension: 0.4,
            fill: true,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => `Sessions: ${context.parsed.y}`,
            },
          },
        },
        scales: {
          y: { beginAtZero: true, title: { display: true, text: 'Sessions' } },
        },
      },
    });
  }
}
