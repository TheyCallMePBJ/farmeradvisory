# Krishi Saathi (कृषी साथी)

Krishi Saathi is an AI-powered WhatsApp chatbot designed to provide real-time agricultural advisory to grape farmers in Maharashtra, India. The current MVP focuses on the grape-growing regions of Nashik, Sangli, and Pune.

Farmers can interact with the bot in Marathi, Hindi, or English using either text messages or WhatsApp voice notes.

---

## Features

- **Multilingual Onboarding**: Interactive buttons and list menus guide farmers through:
  - Preferred language selection (Marathi, Hindi, English)
  - Grape variety selection (Thompson Seedless, Manik Chaman, Sharad Seedless, Sonaka)
  - District selection (Nashik, Sangli, Pune)
  - Fruit pruning date registration (with calendar and range validation)
- **Real-Time Weather Integration**: Connects to the OpenWeather API to fetch current temperature, humidity, rainfall, and wind speeds for the farmer's district. Live weather is used by the AI to evaluate disease risks like Downy Mildew (*केवडा*) and Powdery Mildew (*भुरी*), and advise on spraying suitability.
- **Multimodal AI Advisory**: Farmers can send text queries or voice notes. Powered by Gemini 3.1 Flash-Lite with async processing, strictly matching the user's input language and providing exact dosages per 200L tank or acre.
- **Reliable Concurrency**: Built-in per-user asynchronous locks and message deduplication to handle rapid messaging and webhook retries safely.
- **Health Check Endpoint**: Includes `/health` for easy service monitoring and cloud deployment readiness.

---

## Tech Stack

- **Backend**: FastAPI, Uvicorn
- **AI Model**: Google Gemini (`google-genai` SDK, `gemini-3.1-flash-lite`)
- **Database**: SQLite via `aiosqlite`
- **APIs**: Meta WhatsApp Cloud API (Graph API v21.0), OpenWeather API
- **HTTP Client**: `httpx`

---

## Environment Variables

Create a `.env` file in the project root:

```env
META_ACCESS_TOKEN=your_meta_system_user_access_token
PHONE_NUMBER_ID=your_whatsapp_phone_number_id
WABA_ID=your_messaging_account_id_or_waba_id
VERIFY_TOKEN=krishi_saathi_secret_token_123
OPENWEATHER_API_KEY=your_openweather_api_key
GEMINI_API_KEY=your_gemini_api_key
```

---

## Local Setup

1. **Clone the repository**:
   ```bash
   git clone <repo-url>
   cd krishisaathi
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the server**:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```

5. **Expose locally (for webhook testing)**:
   ```bash
   ngrok http 8000
   ```
   Configure `https://<ngrok-subdomain>.ngrok-free.app/webhook` as your Webhook Callback URL in the Meta Developer Dashboard.

---

## API Endpoints

- `GET /health` or `GET /`: Health check endpoint returning service status and environment variable checks.
- `GET /webhook`: Webhook verification challenge endpoint for Meta WhatsApp Cloud API.
- `POST /webhook`: Webhook endpoint receiving incoming messages, button clicks, and media from WhatsApp.
