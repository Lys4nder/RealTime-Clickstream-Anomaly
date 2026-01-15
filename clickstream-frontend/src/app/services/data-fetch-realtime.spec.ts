import { TestBed } from '@angular/core/testing';

import { DataFetchRealtimeService } from './data-fetch-realtime';

describe('DataFetchRealtime', () => {
  let service: DataFetchRealtimeService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DataFetchRealtimeService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
