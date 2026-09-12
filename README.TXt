to run app on the nas --> SSH into nas
    ssh -p 100 tsoetens@192.168.0.107

als proces draait en killen

pidof python --> zoeken naar tsoetens ID 
KILL "PID nr"

cd /volume1/web/my_P1_dashboard
# Linux/NAS (SSH)
bash setup_ssh_env.sh
source venv39/bin/activate
# Windows PowerShell
# C
nohup python app.py &

================================================================================
NEW: HUAWEI SUN2000 SOLAR INVERTER INTEGRATION
================================================================================

The P1 Dashboard now includes integrated support for Huawei Sun2000 solar inverters!

Quick Start:
1. Read: SUN2000_QUICKSTART.md (5-minute setup)
2. Detailed config: SUN2000_SETUP.md (comprehensive guide)

Features:
- Real-time power monitoring (W)
- Daily/monthly/yearly yield tracking (kWh)
- Inverter health and efficiency metrics
- 30-day data retention with permanent aggregates
- REST API endpoints for custom integration

Environment Variables (NEW):
- `SUN2000_IP`: IP address of your inverter (e.g., 192.168.0.12)
- `SUN2000_PORT`: API port (default: 8080)
- `SUN2000_DEVICE_ID`: Device ID from inverter (hex string)

Example in .env:
  SUN2000_IP=192.168.0.12
  SUN2000_PORT=8080
  SUN2000_DEVICE_ID=7f33471b5b8e45d58f37d6c72c3ac6d0

API Endpoints:
- GET  /sun2000/current    - Current power, yield, status
- GET  /sun2000/daily      - Daily yield history
- GET  /sun2000/info       - Device information
- GET  /sun2000/health     - Connection health check
- POST /sun2000/sync       - Manual data sync (admin token required)

Test It:
  python sun2000_reader.py

================================================================================
DOCKER DRAAIEN OP EEN ANDERE HOST / WEBSITE
================================================================================

- De app leest nu zijn P1-bron uit `P1_URL` in plaats van uit een hardcoded NAS-IP.
- De app ondersteunt reverse proxies en een subpad via `APPLICATION_ROOT`.
- De container bewaart databases onder `/data`; mount daarom altijd een volume.

Snelle start met docker compose:

```bash
docker compose up -d --build
```

Belangrijke variabelen:

- `HOST_PORT`: poort op de host, standaard `8100`
- `P1_URL`: endpoint van de P1-reader, bv. `http://192.168.0.11/api/v1/data`
- `APPLICATION_ROOT`: leeg laten voor een losse host/subdomein, of bv. `/p1-dashboard` als de app onder een subpad van een andere website draait
- `P1_DATA_DIR`: in Docker standaard `/data`
- `SMTP_*`, `EMAIL_*`, `ADMIN_TOKEN`: optioneel, best via `.env`
- `SUN2000_IP`, `SUN2000_PORT`, `SUN2000_DEVICE_ID`: voor Solar-integratie (optioneel)

Reverse proxy onder een subpad:

- Zet `APPLICATION_ROOT=/p1-dashboard`
- Laat je proxy requests doorsturen naar de container en stuur ook `X-Forwarded-Prefix: /p1-dashboard`
- De frontend gebruikt dan automatisch het juiste basispad voor alle API-calls

Health endpoint:

- `GET /health` geeft een simpele runtime-status terug
- `GET /sun2000/health` geeft Sun2000 verbindingsstatus terug (als geconfigureerd)
