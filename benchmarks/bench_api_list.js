import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  vus: 50,
  duration: '30s',
  thresholds: {
    'http_req_duration': ['p(95)<100'],
    'errors': ['rate<0.01'],
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';

export default function () {
  const res = http.get(`${BASE_URL}/contracts`);

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
    'response time < 100ms': (r) => r.timings.duration < 100,
  });

  errorRate.add(!success);

  sleep(0.1);
}
