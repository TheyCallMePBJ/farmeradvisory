import os
import re
from datetime import datetime
import httpx
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, BackgroundTasks, Query
from google import genai
from google.genai import types

import database as db

load_dotenv()

META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

GRAPH_API_URL = f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages"

ai_client = genai.Client(api_key=GEMINI_API_KEY)

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
    await db.init_db()
    yield

app = FastAPI(lifespan=lifespan)

# --- WhatsApp API Outbound Helpers ---
async def send_whatsapp(payload: dict):
    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(GRAPH_API_URL, headers=headers, json=payload, timeout=10.0)
            if r.status_code >= 400:
                print(f"[Meta Outbound Error] {r.status_code}: {r.text}")
        except Exception as e:
            print(f"[Meta Exception]: {e}")

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
    async with httpx.AsyncClient() as client:
        r = await client.get(f"https://graph.facebook.com/v21.0/{media_id}", headers=headers, timeout=10.0)
        data = r.json()
        audio_stream = await client.get(data.get("url"), headers=headers, timeout=20.0)
        return audio_stream.content, data.get("mime_type", "audio/ogg").split(";")[0]

# --- Gemini Query Handler (Fast & Direct) ---
async def query_gemini_fast(farmer: dict, history: list[dict], user_input: str | bytes, mime_type: str = "text/plain") -> str:
    system_instruction = f"""
You are 'Krishi Saathi' (कृषी साथी), a specialist agricultural advisor for grape farmers in Maharashtra, India.
Farmer Details:
- District: {farmer.get('district', 'Nashik')}
- Grape Variety: {farmer.get('variety', 'Thompson Seedless')}
- Pruning Date: {farmer.get('pruning_date', 'Not specified')}

Strict Rules:
1. Always respond in the EXACT same language the user asks in (Marathi, Hindi, or English).
2. Use single asterisks for *bold*. Never use '#', '##', or tables.
3. Keep the reply direct, concise, and under 100 words. Give exact chemical/organic doses per 200L water tank or per acre.
"""
    contents = []
    # Include up to 2 prior turns only to keep prompt lean and fast
    for turn in history[-2:]:
        contents.append(types.Content(
            role="user" if turn["role"] == "user" else "model",
            parts=[types.Part.from_text(text=turn["content"])]
        ))

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

    try:
        # Zero thinking budget avoids internal scratchpad delays
        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=0.2,
            max_output_tokens=350,
            thinking_config=types.ThinkingConfig(thinking_budget=0),
        )
        res = ai_client.models.generate_content(
            model="gemini-3.8-flash",
            contents=contents,
            config=config,
        )
        return res.text.strip()
    except Exception as e:
        print(f"[Gemini Error]: {e}")
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
    farmer = await db.get_farmer(sender)

    # 1. First-time user or manual reset
    text_content = raw_data.get("text", {}).get("body", "").strip() if msg_type == "text" else ""
    if not farmer or text_content.lower() in ["reset", "restart", "start"]:
        await start_language_selection(sender)
        return

    # 2. In Onboarding Flow but when I tried to send a message not too long after receiving a reply for the first, it gave me the exception 
    if not farmer.get("onboarding_complete", 0):
        step = farmer.get("onboarding_step", "AWAITING_LANG")
        lang = farmer.get("language", "mr")
        s = STRINGS.get(lang, STRINGS["mr"])

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
    history = await db.get_recent_history(sender, limit=2)
    user_log = ""
    bot_reply = ""

    if msg_type == "text":
        user_log = text_content
        bot_reply = await query_gemini_fast(farmer, history, text_content)

    elif msg_type == "audio":
        media_id = raw_data.get("audio", {}).get("id")
        audio_bytes, mime = await download_media(media_id)
        user_log = "[Audio Note]"
        bot_reply = await query_gemini_fast(farmer, history, audio_bytes, mime_type=mime)

    else:
        await send_text(sender, "कृपया प्रश्न मजकूर (text) किंवा व्हॉइस नोट (audio) द्वारे पाठवा.")
        return

    await send_text(sender, bot_reply)
    await db.save_chat_turn(sender, "user", user_log)
    await db.save_chat_turn(sender, "model", bot_reply)

# --- Webhook Endpoints ---
@app.get("/webhook")
async def verify(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        return Response(content=hub_challenge, media_type="text/plain", status_code=200)
    return Response(content="Failed", status_code=403)

@app.post("/webhook")
async def receive(request: Request, bg: BackgroundTasks):
    data = await request.json()
    try:
        messages = data.get("entry", [])[0].get("changes", [])[0].get("value", {}).get("messages", [])
        if messages:
            msg = messages[0]
            bg.add_task(dispatch_message, msg.get("from"), msg.get("type"), msg)
    except Exception as e:
        print("Webhook error:", e)
    return {"status": "ok"}