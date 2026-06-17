import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    vus: 50,          
    duration: '30s',  
};

export default function () {
    const deviceId = `moto-${Math.floor(Math.random() * 1000)}`;
    const url = `http://172.17.0.1:8080/devices/${deviceId}/telemetry`;

    const payload = JSON.stringify({
        ts: Date.now(),
        lat: -23.5505 + (Math.random() - 0.5) * 0.1,
        lon: -46.6333 + (Math.random() - 0.5) * 0.1,
        battery: Math.random(),
        ax: (Math.random() - 0.5) * 10,
        ay: (Math.random() - 0.5) * 10,
        az: 9.81 + (Math.random() - 0.5) * 2
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(url, payload, params);

    check(res, {
        'status deve ser 202': (r) => r.status === 202,
        'deve conter X-Instance-Id': (r) => r.headers['X-Instance-Id'] !== undefined,
    });

    sleep(0.01); 
}