import os
import re
import sys
import time
import asyncio
import logging
from collections import defaultdict
from datetime import datetime
import httpx
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, BackgroundTasks, Query
from google import genai
from google.genai import types

import database as db

load_dotenv()

# --- Logging Configuration (Ensures immediate unbuffered output on Render) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("krishi_saathi")

META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

GRAPH_API_URL = f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages"

ai_client = genai.Client(api_key=GEMINI_API_KEY)

# --- Concurrency & Weather Cache ---
user_locks = defaultdict(asyncio.Lock)
processed_msg_ids: dict[str, float] = {}
MSG_ID_TTL = 300  # 5 minutes

# Track last query text and timestamp per sender for rapid repeated message deduplication
last_user_queries: dict[str, tuple[str, float]] = {}

weather_cache: dict[str, dict] = {}
WEATHER_CACHE_TTL = 1800  # 30 minutes

# Shared persistent HTTP client for connection pooling and lower latency
http_client: httpx.AsyncClient | None = None

async def get_http_client() -> httpx.AsyncClient:
    global http_client
    if http_client is None or http_client.is_closed:
        http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=5.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
        )
    return http_client

# --- Multilingual UI Strings Dictionary ---
STRINGS = {
    "mr": {
        "variety_title": "🍇 *कृषी साथी मध्ये स्वागत!*\n\nकृपया आपल्या द्राक्षाची जात निवडा:",
        "btn_variety": "जात निवडा",
        "dist_title": "आपला जिल्हा / परिसर निवडा:",
        "btn_dist": "जिल्हा निवडा",
        "pruning_prompt": "📅 कृपया आपल्या बागेची *छाटणी तारीख (Pruning Date)* लिहा.\n\nस्वरूप: *YYYY-MM-DD* (उदा. `2026-09-20`)",
        "invalid_date": "⚠️ कृपया तारीख अचूक *YYYY-MM-DD* स्वरूपात पाठवा (उदा. `2026-09-20`):",
        "setup_done": "✅ *नोंदणी पूर्ण झाली!*\n\nआता आपण द्राक्ष शेतीसंबंधी कोणताही प्रश्न मेसेज किंवा व्हॉइस नोट (audio) द्वारे विचारू शकता.",
        "districts": [("DIST_NASHIK", "नाशिक"), ("DIST_SANGLI", "सांगली"), ("DIST_PUNE", "पुणे")],
        "varieties": [("VAR_THOMPSON", "थॉमसन सीडलेस"), ("VAR_MANIK", "माणिक चमन"), ("VAR_SHARAD", "शरद सीडलेस"), ("VAR_SONAKA", "सोनाका")]
    },
    "hi": {
        "variety_title": "🍇 *कृषि साथी में स्वागत है!*\n\nकृपया अपनी अंगूर की किस्म चुनें:",
        "btn_variety": "किस्म चुनें",
        "dist_title": "अपना जिला या क्षेत्र चुनें:",
        "btn_dist": "जिला चुनें",
        "pruning_prompt": "📅 कृपया अपनी *कटाई/छंटाई तारीख (Pruning Date)* दर्ज करें।\n\nप्रारूप: *YYYY-MM-DD* (उदा. `2026-09-20`)",
        "invalid_date": "⚠️ कृपया सही प्रारूप *YYYY-MM-DD* में तारीख दर्ज करें (उदा. `2026-09-20`):",
        "setup_done": "✅ *पंजीकरण सफल रहा!*\n\nअब आप अंगूर की खेती के बारे में कोई भी सवाल टेक्स्ट या वॉइस नोट भेजकर पूछ सकते हैं।",
        "districts": [("DIST_NASHIK", "नासिक"), ("DIST_SANGLI", "सांगली"), ("DIST_PUNE", "पुणे")],
        "varieties": [("VAR_THOMPSON", "थॉमसन सीडलेस"), ("VAR_MANIK", "मानिक चमन"), ("VAR_SHARAD", "शरद सीडलेस"), ("VAR_SONAKA", "सोनाका")]
    },
    "en": {
        "variety_title": "🍇 *Welcome to Krishi Saathi!*\n\nPlease select your grape variety:",
        "btn_variety": "Choose Variety",
        "dist_title": "Select your region/district:",
        "btn_dist": "Select District",
        "pruning_prompt": "📅 Please enter your *Fruit Pruning Date* in *YYYY-MM-DD* format (e.g., `2026-09-20`):",
        "invalid_date": "⚠️ Invalid format. Please enter date as *YYYY-MM-DD* (e.g., `2026-09-20`):",
        "setup_done": "✅ *Setup Complete!*\n\nYou can now ask any grape farming question via text or voice note.",
        "districts": [("DIST_NASHIK", "Nashik"), ("DIST_SANGLI", "Sangli"), ("DIST_PUNE", "Pune")],
        "varieties": [("VAR_THOMPSON", "Thompson Seedless"), ("VAR_MANIK", "Manik Chaman"), ("VAR_SHARAD", "Sharad Seedless"), ("VAR_SONAKA", "Sonaka")]
    }
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    logger.info("Starting up Krishi Saathi WhatsApp Bot...")
    await db.init_db()
    logger.info(f"Database initialized: {db.DB_NAME}")
    logger.info(f"Configuration check: META_ACCESS_TOKEN={'SET' if META_ACCESS_TOKEN else 'MISSING'}, PHONE_NUMBER_ID={PHONE_NUMBER_ID or 'MISSING'}, VERIFY_TOKEN={'SET' if VERIFY_TOKEN else 'MISSING'}, GEMINI_API_KEY={'SET' if GEMINI_API_KEY else 'MISSING'}, OPENWEATHER_API_KEY={'SET' if OPENWEATHER_API_KEY else 'MISSING'}")
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=5.0),
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
    )
    yield
    logger.info("Shutting down Krishi Saathi...")
    if http_client and not http_client.is_closed:
        await http_client.aclose()

app = FastAPI(lifespan=lifespan)

# --- Health Check Endpoint (For Render & Browser verification) ---
@app.get("/")
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "Krishi Saathi WhatsApp Bot",
        "phone_number_id_configured": bool(PHONE_NUMBER_ID),
        "meta_token_configured": bool(META_ACCESS_TOKEN),
        "gemini_configured": bool(GEMINI_API_KEY),
        "weather_configured": bool(OPENWEATHER_API_KEY)
    }

# --- WhatsApp API Outbound Helpers ---
async def send_whatsapp(payload: dict):
    if not META_ACCESS_TOKEN or not PHONE_NUMBER_ID:
        logger.error("Cannot send message: META_ACCESS_TOKEN or PHONE_NUMBER_ID is not configured in environment variables!")
        return

    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}", "Content-Type": "application/json"}
    client = await get_http_client()
    try:
        r = await client.post(GRAPH_API_URL, headers=headers, json=payload, timeout=10.0)
        if r.status_code >= 400:
            logger.error(f"[Meta Outbound Error {r.status_code}]: {r.text}")
        else:
            logger.info("[Meta Outbound Success]: WhatsApp message sent successfully")
    except Exception as e:
        logger.exception(f"[Meta Outbound Exception]: {e}")

async def send_text(to: str, text: str):
    await send_whatsapp({"messaging_product": "whatsapp", "to": to, "type": "text", "text": {"body": text}})

async def send_buttons(to: str, body: str, buttons: list):
    btn_objs = [{"type": "reply", "reply": {"id": b_id, "title": b_title}} for b_id, b_title in buttons[:3]]
    payload = {
        "messaging_product": "whatsapp", "to": to, "type": "interactive",
        "interactive": {"type": "button", "body": {"text": body}, "action": {"buttons": btn_objs}}
    }
    await send_whatsapp(payload)

async def send_list(to: str, body: str, btn_text: str, items: list):
    rows = [{"id": r_id, "title": r_title} for r_id, r_title in items]
    payload = {
        "messaging_product": "whatsapp", "to": to, "type": "interactive",
        "interactive": {
            "type": "list", "body": {"text": body},
            "action": {"button": btn_text, "sections": [{"title": "Select", "rows": rows}]}
        }
    }
    await send_whatsapp(payload)

async def download_media(media_id: str) -> tuple[bytes, str]:
    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}"}
    client = await get_http_client()
    r = await client.get(f"https://graph.facebook.com/v21.0/{media_id}", headers=headers, timeout=10.0)
    data = r.json()
    audio_stream = await client.get(data.get("url"), headers=headers, timeout=20.0)
    return audio_stream.content, data.get("mime_type", "audio/ogg").split(";")[0]

# --- Weather API Helper ---
async def get_district_weather(district: str) -> dict | None:
    if not OPENWEATHER_API_KEY:
        return None

    now = time.time()
    cached = weather_cache.get(district)
    if cached and (now - cached["timestamp"] < WEATHER_CACHE_TTL):
        return cached["data"]

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": f"{district},IN",
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }
    try:
        client = await get_http_client()
        r = await client.get(url, params=params, timeout=4.0)
        if r.status_code == 200:
            data = r.json()
            weather_info = {
                "temp": round(data.get("main", {}).get("temp", 0), 1),
                "humidity": data.get("main", {}).get("humidity", 0),
                "condition": data.get("weather", [{}])[0].get("description", "Clear"),
                "rain_1h": data.get("rain", {}).get("1h", 0),
                "wind_speed": round(data.get("wind", {}).get("speed", 0) * 3.6, 1),
            }
            weather_cache[district] = {"timestamp": now, "data": weather_info}
            return weather_info
        else:
            logger.error(f"[OpenWeather Error] {r.status_code}: {r.text}")
    except Exception as e:
        logger.exception(f"[OpenWeather Exception]: {e}")

    return cached["data"] if cached else None

# --- Language Helpers ---
def detect_query_language(text: str, default_lang: str = "mr") -> str:
    if not text or not text.strip():
        return default_lang
    devanagari_chars = len(re.findall(r'[\u0900-\u097F]', text))
    total_chars = len(re.findall(r'\w', text))
    if total_chars > 0 and (devanagari_chars / total_chars) > 0.25:
        # Check for typical Marathi markers or fallback to default_lang
        marathi_markers = ["आहे", "का", "काय", "करावी", "रोगाचा", "पावसामुळे", "झाले", "नाही", "शकत", "बागेत", "कसे", "द्राक्ष", "करावे", "होते", "ळ"]
        if any(marker in text for marker in marathi_markers) or default_lang == "mr":
            return "mr"
        return "hi"
    latin_chars = len(re.findall(r'[A-Za-z]', text))
    if total_chars > 0 and (latin_chars / total_chars) > 0.5:
        return "en"
    return default_lang

def clean_language_output(text: str, target_lang: str) -> str:
    if target_lang in ["mr", "hi"]:
        # Strip common English parenthetical translations or Latin abbreviations that LLMs inject
        text = re.sub(r'\s*\(\s*(?:Downy|Powdery)\s+Mildew\s*\)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*\(\s*(?:WDG|SC|EC|WP)\s*\)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*\(\s*(?:gm|g|ml|l|acre)\s*\)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()

# --- Gemini Query Handler (Fast, Robust & Language-Pure) ---
async def query_gemini_fast(farmer: dict, history: list[dict], user_input: str | bytes, mime_type: str = "text/plain") -> str:
    district = farmer.get("district", "Nashik")
    default_lang = farmer.get("language", "mr")

    if isinstance(user_input, str):
        target_lang = detect_query_language(user_input, default_lang=default_lang)
    else:
        target_lang = default_lang

    weather = await get_district_weather(district)
    if weather:
        weather_text = (
            f"- Current Weather in {district}: Temp {weather['temp']}°C, "
            f"Relative Humidity {weather['humidity']}%, Sky: '{weather['condition']}', "
            f"Rainfall (last 1h): {weather['rain_1h']}mm, Wind: {weather['wind_speed']} km/h"
        )
    else:
        weather_text = f"- District: {district} (Live weather unavailable)"

    if target_lang == "mr":
        lang_rule = """CRITICAL LANGUAGE MANDATE:
- You must respond 100% EXCLUSIVELY in pure Marathi (मराठी) using Devanagari script.
- STRICTLY FORBIDDEN: Do NOT include ANY English words, Latin script letters, or English translations in parentheses or brackets anywhere.
- Write ONLY 'केवडा' (NEVER 'केवडा (Downy Mildew)').
- Write ONLY 'भुरी' (NEVER 'भुरी (Powdery Mildew)').
- Write all chemical formulations, dosages, and units in Devanagari script or pure Marathi words (उदा. '८०% डब्ल्यूडीजी' किंवा '८०% विद्राव्य पावडर', '५% एससी', 'मिली', 'ग्रॅम', 'प्रति २०० लिटर पाणी'). NEVER write Latin abbreviations like 'WDG', 'SC', 'EC', 'ml' in brackets.
- The entire response must read naturally and completely in Marathi."""
    elif target_lang == "hi":
        lang_rule = """CRITICAL LANGUAGE MANDATE:
- You must respond 100% EXCLUSIVELY in pure Hindi (हिंदी) using Devanagari script.
- STRICTLY FORBIDDEN: Do NOT include ANY English words, Latin script letters, or English translations in parentheses or brackets anywhere.
- Write technical terms, disease names, formulations, and units in Devanagari script (उदा. '८०% डब्लूडीजी', '५% एससी', 'मिली', 'ग्राम', 'प्रति २०० लीटर पानी').
- The entire response must read naturally and completely in Hindi."""
    else:
        lang_rule = """CRITICAL LANGUAGE MANDATE:
- You must respond 100% in English."""

    system_instruction = f"""You are 'Krishi Saathi' (कृषी साथी), an expert agricultural viticulture advisor for grape farmers in Maharashtra, India.
Farmer Details:
- District: {district}
{weather_text}
- Grape Variety: {farmer.get('variety', 'Thompson Seedless')}
- Fruit Pruning Date: {farmer.get('pruning_date', 'Not specified')}

{lang_rule}

Strict Formatting & Content Rules:
1. Use single asterisks for *bold*. Never use '#', '##', or tables.
2. Keep the reply direct, concise, and actionable (maximum 60-80 words, 3 to 4 short bullet points).
3. When advising on spraying, always give exact chemical or organic doses per 200L water tank or per acre.
4. Disease Risk & Spraying Guide based on live weather:
   - High humidity (>80%) or rain -> high risk of Downy Mildew (केवडा).
   - Moderate temperature with humid morning canopy -> risk of Powdery Mildew (भुरी).
   - If wind speed > 15 km/h or rain is imminent, explicitly advise postponing chemical spraying.
"""

    contents = []
    # Build clean alternating history (must start with user, must strictly alternate)
    last_role = None
    for turn in history[-4:]:
        role = "user" if turn["role"] == "user" else "model"
        content_text = turn.get("content", "").strip()
        # Skip empty turns or error messages
        if not content_text or content_text.startswith("क्षमस्व") or content_text.startswith("क्षमा") or content_text.startswith("Sorry"):
            continue
        # Gemini requires the first turn in contents to be 'user'
        if not contents and role != "user":
            continue
        # Strict alternation required
        if role == last_role:
            continue
        contents.append(types.Content(
            role=role,
            parts=[types.Part.from_text(text=content_text)]
        ))
        last_role = role

    # Ensure the last history turn is 'model' so the new user turn follows properly
    if contents and contents[-1].role == "user":
        contents.pop()

    if isinstance(user_input, bytes):
        contents.append(types.Content(
            role="user",
            parts=[
                types.Part.from_bytes(data=user_input, mime_type=mime_type),
                types.Part.from_text(text="Concise advice for farmer:")
            ]
        ))
    else:
        contents.append(types.Content(
            role="user",
            parts=[types.Part.from_text(text=user_input)]
        ))

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=0.2,
        max_output_tokens=700,
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )

    for attempt in range(2):
        try:
            res = await asyncio.wait_for(
                ai_client.aio.models.generate_content(
                    model="gemini-3.8-flash",
                    contents=contents,
                    config=config,
                ),
                timeout=12.0
            )

            reply_text = ""
            if res.text:
                reply_text = res.text.strip()
            elif res.candidates and res.candidates[0].content and res.candidates[0].content.parts:
                reply_text = "".join(p.text for p in res.candidates[0].content.parts if getattr(p, "text", None)).strip()

            if reply_text:
                cleaned = clean_language_output(reply_text, target_lang)
                return cleaned

        except asyncio.TimeoutError:
            logger.warning(f"[Gemini Timeout] Attempt {attempt + 1} timed out after 12s")
        except Exception as e:
            logger.exception(f"[Gemini Exception] Attempt {attempt + 1}: {e}")
            if attempt == 0:
                await asyncio.sleep(1.0)

    # Fallback message matching target language
    if target_lang == "hi":
        return "क्षमा करें, उत्तर प्राप्त करने में समस्या आ रही है। कृपया कुछ समय बाद पुनः प्रयास करें।"
    elif target_lang == "en":
        return "Sorry, having trouble generating an advisory right now. Please try again in a moment."
    else:
        return "क्षमस्व, उत्तर मिळवण्यात अडचण येत आहे. कृपया थोड्या वेळाने प्रयत्न करा."

# --- State Machine & Onboarding Flow ---
async def start_language_selection(phone: str):
    await db.save_or_update_farmer(phone, onboarding_complete=0, onboarding_step="AWAITING_LANG")
    buttons = [
        ("LANG_MR", "मराठी"),
        ("LANG_HI", "हिंदी"),
        ("LANG_EN", "English"),
    ]
    await send_buttons(phone, "नमस्कार! Welcome to *Krishi Saathi*.\n\nPlease select your preferred language:", buttons)

def validate_pruning_date(date_str: str) -> bool:
    try:
        parsed_date = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        today = datetime.now().date()
        # Pruning cannot be in the future and should be within the past 2 years (grape pruning season cycles)
        if parsed_date > today or (today - parsed_date).days > 730:
            return False
        return True
    except (ValueError, TypeError):
        return False

async def dispatch_message(sender: str, msg_type: str, raw_data: dict):
    async with user_locks[sender]:
        try:
            logger.info(f"Processing incoming message (type={msg_type})")
            farmer = await db.get_farmer(sender)

            # 1. First-time user or manual reset
            text_content = raw_data.get("text", {}).get("body", "").strip() if msg_type == "text" else ""

            # Deduplicate rapid repeated text queries within 3 seconds
            now = time.time()
            last_text, last_ts = last_user_queries.get(sender, ("", 0.0))
            if msg_type == "text" and text_content and text_content == last_text and (now - last_ts < 3.0):
                logger.info(f"Ignoring duplicate rapid message from {sender}")
                return
            if msg_type == "text" and text_content:
                last_user_queries[sender] = (text_content, now)

            if not farmer or text_content.lower() in ["reset", "restart", "start"]:
                logger.info("Starting onboarding / language selection")
                await start_language_selection(sender)
                return

            # 2. In Onboarding Flow
            if not farmer.get("onboarding_complete", 0):
                step = farmer.get("onboarding_step", "AWAITING_LANG")
                lang = farmer.get("language", "mr")
                s = STRINGS.get(lang, STRINGS["mr"])
                logger.info(f"Onboarding step: {step}")

                # Step 1: Language selection
                if step == "AWAITING_LANG":
                    btn_id = raw_data.get("interactive", {}).get("button_reply", {}).get("id", "")
                    lang_map = {"LANG_MR": "mr", "LANG_HI": "hi", "LANG_EN": "en"}
                    chosen_lang = lang_map.get(btn_id, "mr")
                    await db.save_or_update_farmer(sender, language=chosen_lang, onboarding_step="AWAITING_VARIETY")
                    
                    next_strings = STRINGS[chosen_lang]
                    await send_list(sender, next_strings["variety_title"], next_strings["btn_variety"], next_strings["varieties"])
                    return

                # Step 2: Variety selection
                if step == "AWAITING_VARIETY":
                    list_id = raw_data.get("interactive", {}).get("list_reply", {}).get("id", "")
                    var_map = {"VAR_THOMPSON": "Thompson Seedless", "VAR_MANIK": "Manik Chaman", "VAR_SHARAD": "Sharad Seedless", "VAR_SONAKA": "Sonaka"}
                    chosen_var = var_map.get(list_id, "Thompson Seedless")
                    await db.save_or_update_farmer(sender, variety=chosen_var, onboarding_step="AWAITING_DISTRICT")
                    
                    await send_list(sender, s["dist_title"], s["btn_dist"], s["districts"])
                    return

                # Step 3: District selection
                if step == "AWAITING_DISTRICT":
                    dist_id = raw_data.get("interactive", {}).get("list_reply", {}).get("id", "")
                    d_map = {"DIST_NASHIK": "Nashik", "DIST_SANGLI": "Sangli", "DIST_PUNE": "Pune"}
                    chosen_dist = d_map.get(dist_id, "Nashik")
                    await db.save_or_update_farmer(sender, district=chosen_dist, onboarding_step="AWAITING_PRUNING_DATE")
                    
                    await send_text(sender, s["pruning_prompt"])
                    return

                # Step 4: Pruning date
                if step == "AWAITING_PRUNING_DATE":
                    if msg_type == "text" and validate_pruning_date(text_content):
                        await db.save_or_update_farmer(sender, pruning_date=text_content.strip(), onboarding_complete=1, onboarding_step="COMPLETED")
                        await send_text(sender, s["setup_done"])
                    else:
                        await send_text(sender, s["invalid_date"])
                    return

            # 3. Post-Onboarding: Direct LLM Queries (Text or Voice)
            history = await db.get_recent_history(sender, limit=4)
            user_log = ""
            bot_reply = ""

            if msg_type == "text":
                user_log = text_content
                logger.info("Generating advisory response with Gemini")
                bot_reply = await query_gemini_fast(farmer, history, text_content)

            elif msg_type == "audio":
                media_id = raw_data.get("audio", {}).get("id")
                logger.info("Processing incoming audio note with Gemini")
                audio_bytes, mime = await download_media(media_id)
                user_log = "[Audio Note]"
                bot_reply = await query_gemini_fast(farmer, history, audio_bytes, mime_type=mime)

            else:
                await send_text(sender, "कृपया प्रश्न मजकूर (text) किंवा व्हॉइस नोट (audio) द्वारे पाठवा.")
                return

            await send_text(sender, bot_reply)
            # Only save successful exchanges to history (never corrupt history with error messages)
            if not bot_reply.startswith("क्षमस्व") and not bot_reply.startswith("क्षमा") and not bot_reply.startswith("Sorry"):
                await db.save_chat_exchange(sender, user_log, bot_reply)

        except Exception as e:
            logger.exception(f"Unhandled error in dispatch_message: {e}")
            try:
                await send_text(sender, "क्षमस्व, तांत्रिक अडचण आली आहे. कृपया थोड्या वेळाने प्रयत्न करा.")
            except Exception:
                pass

# --- Webhook Endpoints ---
@app.get("/webhook")
async def verify(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        logger.info("Webhook verification SUCCESSFUL!")
        return Response(content=hub_challenge, media_type="text/plain", status_code=200)
    logger.warning("Webhook verification FAILED (token mismatch)")
    return Response(content="Failed", status_code=403)

@app.post("/webhook")
async def receive(request: Request, bg: BackgroundTasks):
    try:
        data = await request.json()
    except Exception as e:
        logger.error(f"Webhook POST: Invalid JSON received: {e}")
        return {"status": "invalid_json"}

    try:
        entries = data.get("entry", [])
        if not entries:
            return {"status": "ok"}

        for entry in entries:
            for change in entry.get("changes", []):
                value = change.get("value", {})
                messages = value.get("messages", [])

                for msg in messages:
                    msg_id = msg.get("id")
                    sender = msg.get("from")
                    msg_type = msg.get("type")
                    now = time.time()

                    if msg_id:
                        if len(processed_msg_ids) > 1000:
                            cutoff = now - MSG_ID_TTL
                            expired = [mid for mid, ts in processed_msg_ids.items() if ts < cutoff]
                            for mid in expired:
                                del processed_msg_ids[mid]
                        if msg_id in processed_msg_ids:
                            continue
                        processed_msg_ids[msg_id] = now

                    bg.add_task(dispatch_message, sender, msg_type, msg)

    except Exception as e:
        logger.exception(f"Error handling webhook POST: {e}")

    return {"status": "ok"}