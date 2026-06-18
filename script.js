import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// =============================================================================
// CONFIGURAÇÃO
// =============================================================================
// Escolha qual cenário rodar via variável de ambiente SCENARIO:
//   k6 run -e SCENARIO=single load-test.js
//   k6 run -e SCENARIO=batch  load-test.js
//   k6 run -e SCENARIO=mixed  load-test.js   (default, roda os dois juntos)
//
// Outras envs úteis:
//   -e VUS=25            (default 25)
//   -e DURATION=30s       (default 30s)
//   -e BATCH_SIZE=50      (default 50, máximo 100 pelo contrato)
//   -e BASE_URL=http://172.17.0.1:8080  (default)

const SCENARIO = __ENV.SCENARIO || 'mixed';
const VUS = parseInt(__ENV.VUS || '25');
const DURATION = __ENV.DURATION || '30s';
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || '50');
const BASE_URL = __ENV.BASE_URL || 'http://172.17.0.1:8080';

// =============================================================================
// MÉTRICAS CUSTOMIZADAS — separadas por endpoint para diagnóstico real
// =============================================================================
const singleDuration = new Trend('single_duration', true);
const batchDuration = new Trend('batch_duration', true);
const singleErrors = new Counter('single_errors');
const batchErrors = new Counter('batch_errors');
const batchAcceptedMismatch = new Counter('batch_accepted_mismatch');

// =============================================================================
// DEFINIÇÃO DE CENÁRIOS — só os cenários relevantes entram no scenarios{}
// =============================================================================
function buildScenarios() {
    const scenarios = {};

    if (SCENARIO === 'single' || SCENARIO === 'mixed') {
        scenarios.scenario_single = {
            executor: 'constant-vus',
            vus: VUS,
            duration: DURATION,
            exec: 'testSingle',
        };
    }

    if (SCENARIO === 'batch' || SCENARIO === 'mixed') {
        scenarios.scenario_batch = {
            executor: 'constant-vus',
            vus: VUS,
            duration: DURATION,
            exec: 'testBatch',
        };
    }

    return scenarios;
}

export const options = {
    scenarios: buildScenarios(),
    thresholds: {
        'http_req_failed': ['rate<0.01'],       
        'single_duration': ['p(95)<100', 'p(99)<250'],
        'batch_duration': ['p(95)<300', 'p(99)<600'],
        'single_errors': ['count==0'],
        'batch_errors': ['count==0'],
        'batch_accepted_mismatch': ['count==0'],
    },
};

// =============================================================================
// HELPERS
// =============================================================================
function gerarPonto() {
    return {
        ts: Date.now(),
        lat: -23.5505 + (Math.random() - 0.5) * 0.1,
        lon: -46.6333 + (Math.random() - 0.5) * 0.1,
        battery: Math.random(),
        ax: (Math.random() - 0.5) * 10,
        ay: (Math.random() - 0.5) * 10,
        az: 9.81 + (Math.random() - 0.5) * 2,
    };
}

const params = {
    headers: { 'Content-Type': 'application/json' },
};

// =============================================================================
// CENÁRIO: SINGLE POINT
// =============================================================================
export function testSingle() {
    const deviceId = `moto-${Math.floor(Math.random() * 1000)}`;
    const url = `${BASE_URL}/devices/${deviceId}/telemetry`;
    const payload = JSON.stringify(gerarPonto());

    const res = http.post(url, payload, params);
    singleDuration.add(res.timings.duration);

    const ok = check(res, {
        'single: status 202': (r) => r.status === 202,
        'single: tem X-Instance-Id': (r) => r.headers['X-Instance-Id'] !== undefined,
    });

    if (!ok) singleErrors.add(1);

    sleep(0.01);
}

// =============================================================================
// CENÁRIO: BATCH
// =============================================================================
export function testBatch() {
    const deviceId = `moto-${Math.floor(Math.random() * 1000)}`;
    const url = `${BASE_URL}/devices/${deviceId}/telemetry/batch`;

    const pointsArray = [];
    for (let i = 0; i < BATCH_SIZE; i++) {
        pointsArray.push(gerarPonto());
    }

    const payload = JSON.stringify({ points: pointsArray });
    const res = http.post(url, payload, params);
    batchDuration.add(res.timings.duration);

    const ok = check(res, {
        'batch: status 202': (r) => r.status === 202,
        'batch: tem X-Instance-Id': (r) => r.headers['X-Instance-Id'] !== undefined,
        'batch: accepted correto': (r) => {
            try {
                return JSON.parse(r.body).accepted === BATCH_SIZE;
            } catch (e) {
                return false;
            }
        },
    });

    if (!ok) {
        batchErrors.add(1);
        try {
            const body = JSON.parse(res.body);
            if (body.accepted !== BATCH_SIZE) batchAcceptedMismatch.add(1);
        } catch (e) {
            // corpo nem chegou a ser JSON válido — já contabilizado em batchErrors
        }
    }

    sleep(0.01);
}