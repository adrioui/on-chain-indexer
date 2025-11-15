import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '10s', target: 50 },  // Ramp up to 50 users
    { duration: '30s', target: 100 }, // Spike to 100 users
    { duration: '20s', target: 0 },   // Ramp down
  ],
  thresholds: {
    'http_req_duration': ['p(95)<200'], // 95% of requests under 200ms
    'errors': ['rate<0.01'],            // Error rate under 1%
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';

export default function () {
  const payload = JSON.stringify({
    address: `0x${Math.random().toString(16).substr(2, 40)}`, // Random address
    network: 'ethereum',
    start_block: 0,
    abi_json: '[]',
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(`${BASE_URL}/watch`, payload, params);

  const success = check(res, {
    'status is 201 or 400 or 409': (r) => [201, 400, 409].includes(r.status),
    'status is not 5xx': (r) => r.status < 500,
    'response time < 200ms': (r) => r.timings.duration < 200,
  });

  errorRate.add(!success);

  sleep(0.1);
}
